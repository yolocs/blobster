package lock

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Lock-level sentinel errors, matchable with errors.Is.
var (
	// ErrLockHeld is returned by TryAcquire when another live holder owns the
	// lock.
	ErrLockHeld = errors.New("blobster/lock: already held")
	// ErrLockLost reports, via a Lock's Err, that its lease could not be renewed
	// and the lock is no longer held.
	ErrLockLost = errors.New("blobster/lock: lock lost")
	// ErrInvalidKey is returned when a lock key is empty or would escape the
	// lock prefix.
	ErrInvalidKey = errors.New("blobster/lock: invalid key")
)

// Default location, relative to the backend's root, for lock records. It sits
// under the reserved .blobster/ prefix so it never collides with caller keys.
const DefaultPrefix = ".blobster/locks/"

// Default lease timing. The lease is deliberately second-scale: GCS throttles
// writes to ~1/sec per object, and the lock record is a single hot object, so
// sub-second leases are not portable across backends.
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRetryInterval = 1 * time.Second
)

// Record field names stored in the backend (object metadata via FromBucket).
const (
	ownerField = "owner"
	leaseField = "lease"
)

// Locker creates and contends for named locks over a single Backend. It is safe
// for concurrent use. Construct one per backend+location and acquire many
// distinct locks from it by key.
type Locker struct {
	backend Backend
	prefix  string
	lease   time.Duration
	renew   time.Duration
	retry   time.Duration
	clock   func() time.Time
}

// Option configures a Locker.
type Option func(*Locker)

// WithPrefix sets where lock records are stored, relative to the backend's root.
// Defaults to DefaultPrefix (".blobster/locks/"). A trailing slash is added if
// missing. Pass "" to store records directly at the root (not recommended — it
// risks colliding with caller keys).
func WithPrefix(prefix string) Option {
	return func(l *Locker) {
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		l.prefix = prefix
	}
}

// WithLeaseDuration sets how long an acquisition is valid before it must be
// renewed. A crashed holder's lock becomes acquirable this long after its last
// successful renew. Defaults to DefaultLeaseDuration.
func WithLeaseDuration(d time.Duration) Option {
	return func(l *Locker) { l.lease = d }
}

// WithRenewInterval sets how often a held lock renews its lease in the
// background. It must be comfortably shorter than the lease duration to absorb
// transient backend errors and clock skew. Defaults to one third of the lease.
func WithRenewInterval(d time.Duration) Option {
	return func(l *Locker) { l.renew = d }
}

// WithRetryInterval sets the base poll interval the blocking Acquire waits
// between attempts while a lock is held. The actual wait is jittered. Defaults
// to DefaultRetryInterval.
func WithRetryInterval(d time.Duration) Option {
	return func(l *Locker) { l.retry = d }
}

// WithClock injects the time source used for lease deadlines, mainly for tests.
// Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(l *Locker) { l.clock = now }
}

// New returns a Locker over backend. Use FromBucket to build a Backend from any
// blobster driver.
func New(backend Backend, opts ...Option) *Locker {
	l := &Locker{
		backend: backend,
		prefix:  DefaultPrefix,
		lease:   DefaultLeaseDuration,
		retry:   DefaultRetryInterval,
		clock:   time.Now,
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.lease <= 0 {
		l.lease = DefaultLeaseDuration
	}
	if l.renew <= 0 {
		l.renew = l.lease / 3
	}
	if l.renew <= 0 {
		l.renew = l.lease
	}
	if l.retry <= 0 {
		l.retry = DefaultRetryInterval
	}
	return l
}

// AcquireOption configures a single acquisition.
type AcquireOption func(*acquireConfig)

type acquireConfig struct {
	owner string
}

// WithOwner sets the holder identity recorded in the lock. It is advisory
// (useful in logs and for "who holds this") and not required for correctness.
// When omitted, a random owner is generated per acquisition. Owners should be
// unique per holder.
func WithOwner(owner string) AcquireOption {
	return func(c *acquireConfig) { c.owner = owner }
}

// TryAcquire makes a single attempt to acquire the lock named key. It returns a
// held Lock on success, ErrLockHeld if a live holder owns it, or another error
// on backend failure.
func (l *Locker) TryAcquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error) {
	path, err := l.path(key)
	if err != nil {
		return nil, err
	}
	owner, err := resolveOwner(opts)
	if err != nil {
		return nil, err
	}
	return l.tryAcquire(ctx, key, path, owner)
}

// Acquire blocks until the lock named key is acquired or ctx is done, retrying
// with a jittered poll while the lock is held by someone else.
func (l *Locker) Acquire(ctx context.Context, key string, opts ...AcquireOption) (*Lock, error) {
	path, err := l.path(key)
	if err != nil {
		return nil, err
	}
	owner, err := resolveOwner(opts)
	if err != nil {
		return nil, err
	}
	for {
		held, err := l.tryAcquire(ctx, key, path, owner)
		if err == nil {
			return held, nil
		}
		if !errors.Is(err, ErrLockHeld) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(l.jitteredRetry()):
		}
	}
}

func (l *Locker) tryAcquire(ctx context.Context, key, path, owner string) (*Lock, error) {
	fields, version, err := l.backend.Get(ctx, path)
	now := l.clock()
	switch {
	case errors.Is(err, ErrNotExist):
		newVersion, cerr := l.backend.Create(ctx, path, l.record(owner, now))
		if errors.Is(cerr, ErrExists) {
			return nil, ErrLockHeld
		}
		if cerr != nil {
			return nil, cerr
		}
		return l.start(key, path, owner, newVersion, now.Add(l.lease)), nil
	case err != nil:
		return nil, err
	default:
		if !expired(fields, now) {
			return nil, ErrLockHeld
		}
		// Record exists but its lease has lapsed: take over with a CAS on the
		// stale version. A losing racer (or a renew that just landed) trips the
		// conflict and backs off.
		newVersion, uerr := l.backend.Update(ctx, path, l.record(owner, now), version)
		if errors.Is(uerr, ErrConflict) || errors.Is(uerr, ErrNotExist) {
			return nil, ErrLockHeld
		}
		if uerr != nil {
			return nil, uerr
		}
		return l.start(key, path, owner, newVersion, now.Add(l.lease)), nil
	}
}

func (l *Locker) record(owner string, now time.Time) map[string]string {
	return map[string]string{
		ownerField: owner,
		leaseField: now.Add(l.lease).Format(time.RFC3339Nano),
	}
}

// expired reports whether a record's lease deadline is at or before now. A
// record with a missing or unparseable lease is treated as expired so a
// malformed record can always be recovered.
func expired(fields map[string]string, now time.Time) bool {
	raw, ok := fields[leaseField]
	if !ok {
		return true
	}
	until, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return true
	}
	return !now.Before(until)
}

func (l *Locker) jitteredRetry() time.Duration {
	// Full jitter in [retry, 2*retry) to spread contenders off the hot object.
	return l.retry + time.Duration(rand.Int64N(int64(l.retry)+1))
}

func (l *Locker) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	return l.prefix + key, nil
}

func resolveOwner(opts []AcquireOption) (string, error) {
	var cfg acquireConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.owner != "" {
		return cfg.owner, nil
	}
	return randomOwner()
}

func randomOwner() (string, error) {
	var buf [16]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// Lock is a held lock. Its lease is renewed in the background until Release is
// called or the lease is lost. A holder that drops a Lock without releasing it
// leaks the background renewer and keeps the lock held until the process exits,
// so always Release.
type Lock struct {
	locker *Locker
	key    string
	path   string
	owner  string

	cancelRenew context.CancelFunc

	mu         sync.Mutex
	version    string
	leaseUntil time.Time
	done       chan struct{}
	err        error
	finished   bool
	released   bool
}

func (l *Locker) start(key, path, owner, version string, leaseUntil time.Time) *Lock {
	ctx, cancel := context.WithCancel(context.Background())
	lk := &Lock{
		locker:      l,
		key:         key,
		path:        path,
		owner:       owner,
		cancelRenew: cancel,
		version:     version,
		leaseUntil:  leaseUntil,
		done:        make(chan struct{}),
	}
	go lk.renewLoop(ctx)
	return lk
}

// Key returns the lock's key.
func (lk *Lock) Key() string { return lk.key }

// Owner returns the holder identity recorded in the lock.
func (lk *Lock) Owner() string { return lk.owner }

// Done returns a channel closed when the lock is no longer held — either
// released by the caller or lost (lease could not be renewed). After it closes,
// Err reports which.
func (lk *Lock) Done() <-chan struct{} { return lk.done }

// Err returns nil while the lock is held or after a clean Release, and
// ErrLockLost if the lease was lost. It is meaningful once Done is closed.
func (lk *Lock) Err() error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.err
}

// Release relinquishes the lock, stopping the renewer and deleting the record
// if it still belongs to this holder. It is safe to call more than once and
// never deletes a lock that has since been taken over by another holder.
func (lk *Lock) Release(ctx context.Context) error {
	lk.mu.Lock()
	if lk.released {
		lk.mu.Unlock()
		return nil
	}
	lk.released = true
	version := lk.version
	lk.mu.Unlock()

	lk.cancelRenew()
	lk.finish(nil)

	err := lk.locker.backend.Delete(ctx, lk.path, version)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotExist) {
		// Already taken over or gone: our CAS deleted nothing, which is exactly
		// the safety we want. We no longer hold it, so report success.
		return nil
	}
	return err
}

func (lk *Lock) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(lk.locker.renew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := lk.locker.clock()
		lk.mu.Lock()
		leaseUntil := lk.leaseUntil
		lk.mu.Unlock()
		if !now.Before(leaseUntil) {
			// The lease has run out before we could renew it; we must assume a
			// successor may take over and stop claiming the lock.
			lk.finish(ErrLockLost)
			return
		}

		err := lk.renewOnce(ctx, now)
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotExist) {
			lk.finish(ErrLockLost)
			return
		}
		// A transient error is not fatal on its own: keep trying on the next
		// tick until either it clears or the lease deadline above is crossed.
	}
}

func (lk *Lock) renewOnce(ctx context.Context, now time.Time) error {
	lk.mu.Lock()
	version := lk.version
	lk.mu.Unlock()

	newVersion, err := lk.locker.backend.Update(ctx, lk.path, lk.locker.record(lk.owner, now), version)
	if err != nil {
		return err
	}
	lk.mu.Lock()
	lk.version = newVersion
	lk.leaseUntil = now.Add(lk.locker.lease)
	lk.mu.Unlock()
	return nil
}

func (lk *Lock) finish(err error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if lk.finished {
		return
	}
	lk.finished = true
	lk.err = err
	close(lk.done)
}
