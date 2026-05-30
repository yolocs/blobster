package blobster

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

// Lock sentinel errors, matchable with errors.Is.
var (
	// ErrLockHeld is returned by TryAcquire when another live holder owns the
	// lock.
	ErrLockHeld = errors.New("blobster: lock already held")
	// ErrLockLost reports, via a Lock's Err, that its lease could not be renewed
	// and the lock is no longer held.
	ErrLockLost = errors.New("blobster: lock lost")
	// ErrInvalidLockKey is returned when a lock key is empty or would escape the
	// lock prefix.
	ErrInvalidLockKey = errors.New("blobster: invalid lock key")
)

// DefaultLockPrefix is the default location, relative to the bucket's root, for
// lock records. It sits under the reserved .blobster/ prefix so it never
// collides with caller keys.
const DefaultLockPrefix = ".blobster/locks/"

// Default lease timing. The lease is deliberately second-scale: GCS throttles
// writes to ~1/sec per object, and the lock record is a single hot object, so
// sub-second leases are not portable across backends.
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRetryInterval = 1 * time.Second
)

// Record field names stored in the bucket as the lock object's user metadata.
// They must be valid, verbatim-round-tripping metadata names on every backend;
// both are already lowercase C# identifiers, which all drivers (including the
// escaping azureblob driver) preserve.
const (
	lockOwnerField = "owner"
	lockLeaseField = "lease"
)

// Locker creates and contends for named locks over a single Bucket. It is safe
// for concurrent use. Construct one per bucket+location with NewLocker and
// acquire many distinct locks from it by key.
//
// Lock state lives in the lock object's user metadata (owner + an absolute lease
// deadline), so a single Attributes read returns both the state and the opaque
// version token that drives the compare-and-swap. The version token is never
// exposed to callers. The bucket must advertise ConditionalWrites; every
// production driver does. A caller builds a native client once and uses it for
// both blob operations and locking by passing the same driver here.
//
// This is a lease lock, not a fencing lock. It provides mutual exclusion in the
// common case and self-heals on crash (a dead holder's lease expires and the
// next acquirer takes over), but it does NOT protect against a holder that
// pauses past its lease and then resumes — a frozen process cannot observe its
// own freeze, so the loss signal on a Lock is liveness, never a safety guard.
// Callers whose critical section must be safe under arbitrary pauses are
// responsible for idempotency (keep sections short, make external effects
// idempotent, or guard a protected blob with its own conditional write, whose
// version token already orders writers).
type Locker struct {
	bucket Bucket
	prefix string
	lease  time.Duration
	renew  time.Duration
	retry  time.Duration
	clock  func() time.Time
}

// LockerOption configures a Locker.
type LockerOption func(*Locker)

// WithLockPrefix sets where lock records are stored, relative to the bucket's
// root. Defaults to DefaultLockPrefix (".blobster/locks/"). A trailing slash is
// added if missing. Pass "" to store records directly at the root (not
// recommended — it risks colliding with caller keys).
func WithLockPrefix(prefix string) LockerOption {
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
func WithLeaseDuration(d time.Duration) LockerOption {
	return func(l *Locker) { l.lease = d }
}

// WithRenewInterval sets how often a held lock renews its lease in the
// background. It must be comfortably shorter than the lease duration to absorb
// transient backend errors and clock skew. Defaults to one third of the lease.
func WithRenewInterval(d time.Duration) LockerOption {
	return func(l *Locker) { l.renew = d }
}

// WithRetryInterval sets the base poll interval the blocking Acquire waits
// between attempts while a lock is held. The actual wait is jittered. Defaults
// to DefaultRetryInterval.
func WithRetryInterval(d time.Duration) LockerOption {
	return func(l *Locker) { l.retry = d }
}

// WithLockClock injects the time source used for lease deadlines, mainly for
// tests. It should track wall-clock rate, since lease deadlines are compared
// against it on every host. Defaults to time.Now.
func WithLockClock(now func() time.Time) LockerOption {
	return func(l *Locker) { l.clock = now }
}

// NewLocker returns a Locker that stores its records in bucket. Pass any
// blobster driver (mem, file, s3, gcs, azureblob); the lock works on any bucket
// that advertises ConditionalWrites.
func NewLocker(bucket Bucket, opts ...LockerOption) *Locker {
	l := &Locker{
		bucket: bucket,
		prefix: DefaultLockPrefix,
		lease:  DefaultLeaseDuration,
		retry:  DefaultRetryInterval,
		clock:  time.Now,
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

// LockOption configures a single acquisition.
type LockOption func(*acquireConfig)

type acquireConfig struct {
	owner string
}

// WithLockOwner sets the holder identity recorded in the lock. It is advisory
// (useful in logs and for "who holds this") and not required for correctness,
// though Release relies on it to recognize its own record, so owners should be
// unique per holder. When omitted, a random owner is generated per acquisition.
func WithLockOwner(owner string) LockOption {
	return func(c *acquireConfig) { c.owner = owner }
}

// TryAcquire makes a single attempt to acquire the lock named key. It returns a
// held Lock on success, ErrLockHeld if a live holder owns it, or another error
// on backend failure.
//
// The returned Lock renews its lease on a background goroutine that runs on an
// internal context, independent of ctx: it is stopped only by Release or by
// losing the lease, not by canceling ctx (which governs only this call).
func (l *Locker) TryAcquire(ctx context.Context, key string, opts ...LockOption) (*Lock, error) {
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
// with a jittered poll while the lock is held by someone else. See TryAcquire
// for the background renewer's lifecycle.
func (l *Locker) Acquire(ctx context.Context, key string, opts ...LockOption) (*Lock, error) {
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
	fields, version, err := l.getRecord(ctx, path)
	now := l.clock()
	switch {
	case errors.Is(err, ErrNotFound):
		newVersion, cerr := l.createRecord(ctx, path, l.record(owner, now))
		if errors.Is(cerr, ErrPreconditionFailed) {
			return nil, ErrLockHeld
		}
		if cerr != nil {
			return nil, cerr
		}
		return l.start(key, path, owner, newVersion, now.Add(l.lease)), nil
	case err != nil:
		return nil, err
	default:
		if !lockExpired(fields, now) {
			return nil, ErrLockHeld
		}
		// Record exists but its lease has lapsed: take over with a CAS on the
		// stale version. A losing racer (or a renew that just landed) trips the
		// precondition and backs off.
		newVersion, uerr := l.updateRecord(ctx, path, l.record(owner, now), version)
		if errors.Is(uerr, ErrPreconditionFailed) || errors.Is(uerr, ErrNotFound) {
			return nil, ErrLockHeld
		}
		if uerr != nil {
			return nil, uerr
		}
		return l.start(key, path, owner, newVersion, now.Add(l.lease)), nil
	}
}

// getRecord reads a lock record's fields and version, returning ErrNotFound if
// it is absent.
func (l *Locker) getRecord(ctx context.Context, path string) (map[string]string, string, error) {
	attrs, err := l.bucket.Attributes(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return attrs.Metadata, attrs.Version, nil
}

// createRecord writes a record only if absent (ErrPreconditionFailed if it
// exists) and returns its new version.
func (l *Locker) createRecord(ctx context.Context, path string, fields map[string]string) (string, error) {
	return l.writeRecord(ctx, path, fields, IfNotExists)
}

// updateRecord rewrites a record only if its version still matches expected
// (ErrPreconditionFailed otherwise) and returns its new version.
func (l *Locker) updateRecord(ctx context.Context, path string, fields map[string]string, expected string) (string, error) {
	return l.writeRecord(ctx, path, fields, IfMatch(expected))
}

func (l *Locker) writeRecord(ctx context.Context, path string, fields map[string]string, precondition Precondition) (string, error) {
	opts := &WriterOptions{Metadata: fields, DisableContentTypeDetection: true}
	if err := l.bucket.WriteAll(ctx, path, nil, opts, precondition); err != nil {
		return "", err
	}
	// The write path does not return the new version token, so read it back for
	// the next compare-and-swap. We hold the record at this point (a create
	// just succeeded, or our CAS did), so the read cannot observe a foreign
	// write within the lease.
	attrs, err := l.bucket.Attributes(ctx, path)
	if err != nil {
		return "", err
	}
	return attrs.Version, nil
}

func (l *Locker) deleteRecord(ctx context.Context, path, expected string) error {
	return l.bucket.Delete(ctx, path, IfMatch(expected))
}

func (l *Locker) record(owner string, now time.Time) map[string]string {
	return map[string]string{
		lockOwnerField: owner,
		lockLeaseField: now.Add(l.lease).Format(time.RFC3339Nano),
	}
}

// lockExpired reports whether a record's lease deadline is at or before now. A
// record with a missing or unparseable lease is treated as expired so a
// malformed record can always be recovered.
func lockExpired(fields map[string]string, now time.Time) bool {
	raw, ok := fields[lockLeaseField]
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
	// Full jitter in [retry, 2*retry] to spread contenders off the hot object.
	return l.retry + time.Duration(rand.Int64N(int64(l.retry)+1))
}

func (l *Locker) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty key", ErrInvalidLockKey)
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", fmt.Errorf("%w: %q", ErrInvalidLockKey, key)
	}
	return l.prefix + key, nil
}

func resolveOwner(opts []LockOption) (string, error) {
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
	stopped     chan struct{} // closed when the renewer goroutine has exited

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
		stopped:     make(chan struct{}),
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

// Release relinquishes the lock: it stops the renewer, waits for it to exit so
// no further renew can race, and deletes the record if it still belongs to this
// holder. It never deletes a lock taken over by another holder, and is safe to
// call more than once (including after the lease was lost). The returned error
// is authoritative for whether cleanup succeeded; Err is reserved for the
// held-time lost-vs-clean distinction.
func (lk *Lock) Release(ctx context.Context) error {
	lk.mu.Lock()
	if lk.released {
		lk.mu.Unlock()
		return nil
	}
	lk.released = true
	lk.mu.Unlock()

	lk.cancelRenew()
	<-lk.stopped // the renewer has exited; no more writes can race the delete
	lk.finish(nil)

	// The renewer's final Update may have committed a newer version even if its
	// context was canceled mid-flight, so trust the backend rather than our
	// cached version. Only delete a record that is still ours.
	fields, version, err := lk.locker.getRecord(ctx, lk.path)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if fields[lockOwnerField] != lk.owner {
		return nil // taken over by another holder; nothing of ours to delete
	}
	if err := lk.locker.deleteRecord(ctx, lk.path, version); err != nil {
		if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (lk *Lock) renewLoop(ctx context.Context) {
	defer close(lk.stopped)
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
			// The lease ran out before we could renew it; we must assume a
			// successor may take over and stop claiming the lock.
			lk.finish(ErrLockLost)
			return
		}

		err := lk.renewOnce(ctx, now)
		if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrNotFound) {
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

	newVersion, err := lk.locker.updateRecord(ctx, lk.path, lk.locker.record(lk.owner, now), version)
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
