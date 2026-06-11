package blobster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidWatchName is returned by NewWatcher when the watch name is empty,
// is not a single path segment, or would escape its subtree.
var ErrInvalidWatchName = errors.New("blobster: invalid watch name")

// DefaultWatchName is the watch name used when WithWatchName is not given. Two
// watchers bridging different sources into the same bucket must carry distinct
// names, or they will contend for one lock and clobber one cursor.
const DefaultWatchName = "default"

// Watcher bookkeeping locations. The cursor lives in the locker's bucket — the
// follower's own, writable bucket — under the reserved .blobster/ prefix; the
// lock key is namespaced under the locker's configured prefix.
const (
	watchCursorPrefix = ".blobster/watch/"
	watchLockPrefix   = "watch/"
)

// watchPageSize bounds how many source messages one tail page surfaces between
// cursor persists.
const watchPageSize = 256

// Watcher bridges a read-only source queue into a local queue: it tails the
// source's message log forward from a durable cursor and re-enqueues each
// message locally under the source's id, so existing workers consume the
// replicated messages normally. It is the fan-out replication bridge — one
// leader enqueues each message once into its broadcast log, and every follower
// region runs a Watcher that tails that log and fans in locally, keeping the
// leader's write cost O(1) in the number of followers.
//
// A Watcher is a singleton per watch name, made highly available by the lease
// lock: every instance calls Run, exactly one holds the lock and replicates,
// the rest stand by blocked on acquisition, and when the holder dies its lease
// expires and a standby takes over, resuming from the durable cursor. The
// cursor is persisted in the locker's bucket (the follower's own) with a
// compare-and-swap, so a paused stale holder that wakes after a takeover trips
// the cursor's precondition, stands down, and can never rewind a successor's
// progress; the "enqueue, then advance cursor" step is non-atomic but
// idempotent, because the dedup-keyed re-enqueue (EnqueueWithID) makes replays
// no-ops while the message still exists locally.
//
// Delivery is at-least-once end to end: a replicated message that local
// workers already acked can reappear only if the cursor is rewound past it by
// hand — the Watcher itself never rewinds. Payload bytes flow through the
// Watcher as a plain cross-bucket streamed read+write, so source and local may
// be different providers.
type Watcher struct {
	source  *Tail
	local   *Queue
	locker  *Locker
	bucket  Bucket // the locker's bucket; holds the durable cursor
	name    string
	minPoll time.Duration
	maxPoll time.Duration
}

// WatcherOption configures a Watcher.
type WatcherOption func(*Watcher)

// WithWatchName names the watch. The name keys both the singleton lock and the
// durable cursor, so all instances of one logical watch must share it, and
// distinct watches over one bucket must not. Defaults to DefaultWatchName.
func WithWatchName(name string) WatcherOption {
	return func(w *Watcher) { w.name = name }
}

// WithWatchPollBackoff sets the idle poll backoff: when the tail surfaces
// nothing new, the wait starts at low and doubles (jittered) up to high until
// the source moves again. Defaults to DefaultMinPollInterval and
// DefaultMaxPollInterval.
func WithWatchPollBackoff(low, high time.Duration) WatcherOption {
	return func(w *Watcher) {
		w.minPoll = low
		w.maxPoll = high
	}
}

// NewWatcher returns a Watcher that replicates source into local. source is a
// read-only tail over the origin queue (build it with Queue.Tail, typically
// over a bucket this region can only read); local is the queue workers consume
// from; locker elects the singleton holder and its bucket — the follower's own
// — also stores the watch cursor at .blobster/watch/<name>/cursor, because the
// election and the cursor must live together for a takeover to resume exactly
// where the previous holder stopped.
func NewWatcher(source *Tail, local *Queue, locker *Locker, opts ...WatcherOption) (*Watcher, error) {
	if source == nil || local == nil || locker == nil {
		return nil, fmt.Errorf("%w: NewWatcher requires a source tail, a local queue, and a locker", ErrInvalidOption)
	}
	w := &Watcher{
		source:  source,
		local:   local,
		locker:  locker,
		bucket:  locker.bucket,
		name:    DefaultWatchName,
		minPoll: DefaultMinPollInterval,
		maxPoll: DefaultMaxPollInterval,
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.name == "" || strings.Contains(w.name, "/") || strings.Contains(w.name, "..") || w.name == "." {
		return nil, fmt.Errorf("%w: %q", ErrInvalidWatchName, w.name)
	}
	if w.minPoll <= 0 {
		w.minPoll = DefaultMinPollInterval
	}
	if w.maxPoll < w.minPoll {
		w.maxPoll = w.minPoll
	}
	return w, nil
}

func (w *Watcher) cursorPath() string { return watchCursorPrefix + w.name + "/cursor" }
func (w *Watcher) lockKey() string    { return watchLockPrefix + w.name }

// Run contends for the watch lock and, while holding it, tails the source
// forward from the durable cursor, re-enqueues each message locally, and
// persists the cursor after every page, backing off (jittered) while the
// source is idle. Instances that do not hold the lock stand by inside Run;
// losing the lease (or finding the cursor moved by a successor) sends the
// holder back to standing by, so every instance simply runs Run for the
// process's lifetime. Run returns ctx.Err() when ctx is done and surfaces any
// backend error — it does not retry storage failures internally, so a
// supervisor that wants the watch to survive them restarts Run.
func (w *Watcher) Run(ctx context.Context) error {
	for {
		if err := w.runAsHolder(ctx); err != nil {
			return err
		}
		// The lease was lost or a successor moved the cursor: stand by and
		// contend for the lock again.
	}
}

func (w *Watcher) runAsHolder(ctx context.Context) error {
	lock, err := w.locker.Acquire(ctx, w.lockKey())
	if err != nil {
		return err
	}
	// Release on the way out even when ctx is already canceled, so a clean
	// shutdown frees the lock for a standby instead of stalling it for a full
	// lease expiry.
	defer lock.Release(context.WithoutCancel(ctx))

	cursor, version, err := w.readCursor(ctx)
	if err != nil {
		return err
	}

	backoff := w.minPoll
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lock.Done():
			return nil
		default:
		}

		msgs, next, err := w.source.Page(ctx, cursor, watchPageSize)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if err := w.replicate(ctx, m); err != nil {
				return err
			}
		}
		if next == cursor {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-lock.Done():
				return nil
			case <-time.After(queueJitter(backoff)):
			}
			if backoff < w.maxPoll {
				backoff = min(backoff*2, w.maxPoll)
			}
			continue
		}
		cursor = next
		version, err = w.writeCursor(ctx, cursor, version)
		if errors.Is(err, ErrPreconditionFailed) {
			// The cursor moved under us: a successor took over while we were
			// paused. Its progress is ahead of ours, so stand down rather than
			// rewind it; the messages we just enqueued were create-only no-ops.
			return nil
		}
		if err != nil {
			return err
		}
		backoff = w.minPoll
	}
}

// replicate copies one source message into the local queue under the source's
// id, streaming the payload (cross-bucket, provider-agnostic). A source
// message that vanished since listing — acked by an origin consumer or trimmed
// by retention — is skipped: it fell off the log, and the reconciliation sweep
// is the longstop for anything the fast path misses.
func (w *Watcher) replicate(ctx context.Context, m *TailMessage) error {
	attrs, err := m.Attributes(ctx)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	body, err := m.Body(ctx)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = w.local.EnqueueWithID(ctx, m.ID(), body, &EnqueueOptions{Attributes: attrs})
	return err
}

func (w *Watcher) readCursor(ctx context.Context) (cursor, version string, err error) {
	attrs, err := w.bucket.Attributes(ctx, w.cursorPath())
	if errors.Is(err, ErrNotFound) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	b, err := w.bucket.ReadAll(ctx, w.cursorPath())
	if err != nil {
		return "", "", err
	}
	// A foreign write between the two reads leaves a fresh body with a stale
	// version; the first cursor CAS then fails and the holder stands down, so
	// the mismatch self-heals.
	return string(b), attrs.Version, nil
}

// writeCursor persists the cursor under a compare-and-swap on the version read
// at acquisition (create-only the first time), returning the new version for
// the next swap. Like the lock, the write path does not return the version
// token, so it is read back; we hold the cursor at this point, so the read
// cannot observe a foreign write.
func (w *Watcher) writeCursor(ctx context.Context, cursor, version string) (string, error) {
	precondition := IfNotExists
	if version != "" {
		precondition = IfMatch(version)
	}
	opts := &WriterOptions{DisableContentTypeDetection: true}
	if err := w.bucket.WriteAll(ctx, w.cursorPath(), []byte(cursor), opts, precondition); err != nil {
		return "", err
	}
	attrs, err := w.bucket.Attributes(ctx, w.cursorPath())
	if err != nil {
		return "", err
	}
	return attrs.Version, nil
}
