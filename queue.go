package blobster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Queue sentinel errors, matchable with errors.Is.
var (
	// ErrNoMessages is returned by Queue.TryReceive when no message is available
	// to claim — the queue is empty, or every listed message is currently leased.
	ErrNoMessages = errors.New("blobster: no messages available")
	// ErrMessageLost reports, via a Message's Err, that its lease could not be
	// renewed and the message is no longer held; another worker may redeliver it.
	ErrMessageLost = errors.New("blobster: message lost")
	// ErrInvalidQueuePrefix is returned by NewQueue when the prefix is empty or
	// would escape its subtree.
	ErrInvalidQueuePrefix = errors.New("blobster: invalid queue prefix")
)

// Default queue lease timing. The lease is second-scale for the same reason the
// lock's is: GCS throttles writes to ~1/sec per object name, and the lease record
// is a single hot object, so sub-second leases are not portable across backends.
const (
	DefaultVisibilityLease = 15 * time.Second
	DefaultHeadWindow      = 256
	DefaultMinPollInterval = 250 * time.Millisecond
	DefaultMaxPollInterval = 5 * time.Second
)

// Sub-prefixes, relative to the queue's owned prefix, that separate the immutable
// payloads from the mutating lease records, and hold dead-lettered messages.
const (
	queueMsgPrefix   = "msg/"
	queueLeasePrefix = "lease/"
	queueDeadPrefix  = "dead/"
)

// Lease record field names, stored as the lease object's user metadata. They are
// lowercase identifiers that round-trip verbatim on every backend (including the
// escaping azureblob driver), exactly like the lock's record fields.
const (
	queueOwnerField    = "owner"
	queueLeaseField    = "lease"
	queueReceivesField = "receives"
)

// queueJitterWindow bounds how far from the head of the listing a claim attempt
// may start. Workers iterate the head window in lexical (≈FIFO) order but begin
// at a random offset within this front window, so they prefer the earliest
// messages without all stampeding the single lexically-first one.
const queueJitterWindow = 16

// Queue is a work queue built solely on blob storage: competing consumers,
// at-least-once delivery, and approximate FIFO, with no broker, database, or
// coordinator beyond the bucket's conditional-write primitive. It is the next
// coordination primitive after the lease lock and, like the lock, lives in the
// root package and reuses that algorithm — here applied per message.
//
// Each message is two objects under a caller-supplied prefix the queue owns
// wholesale:
//
//	<prefix>/msg/<id>     the payload — written once at enqueue, immutable,
//	                      streamed at any size; user attributes live in its metadata
//	<prefix>/lease/<id>   the lease record — empty body, metadata {owner, lease,
//	                      receives}; the per-message "lock"
//
// Splitting payload from lease means a held message's heartbeat rewrites only the
// tiny, empty-bodied lease record and never re-uploads the payload, so payloads
// stream without buffering and the renew path is exactly the lock's.
//
// The model is at-least-once: a worker that crashes mid-processing has its lease
// expire and the message redelivered, so handlers must be idempotent — the same
// liveness-not-safety contract the lock documents. Ordering is approximate: ids
// are an epoch-millisecond timestamp plus a random UUID (lexically sortable by
// time), and workers prefer the lexically-earliest available message, but
// concurrency, clock skew, and redelivery make it best-effort only.
//
// A Queue is safe for concurrent use; construct one per prefix with NewQueue and
// share it across workers. The bucket must advertise ConditionalWrites.
type Queue struct {
	bucket      Bucket // rooted at the queue's prefix via Sub
	prefix      string // the caller-supplied prefix, normalized with a trailing slash
	lease       time.Duration
	renew       time.Duration
	headWindow  int
	minPoll     time.Duration
	maxPoll     time.Duration
	maxReceives int
	clock       func() time.Time
}

// QueueOption configures a Queue.
type QueueOption func(*Queue)

// WithQueueVisibilityLease sets how long a claimed message stays invisible to other
// workers before its lease must be renewed. A crashed worker's message becomes
// redeliverable this long after its last successful renew. Defaults to
// DefaultVisibilityLease.
func WithQueueVisibilityLease(d time.Duration) QueueOption {
	return func(q *Queue) { q.lease = d }
}

// WithQueueRenewInterval sets how often a held message renews its lease in the
// background. It must be comfortably shorter than the lease to absorb transient
// backend errors and clock skew. Defaults to one third of the lease.
func WithQueueRenewInterval(d time.Duration) QueueOption {
	return func(q *Queue) { q.renew = d }
}

// WithQueueHeadWindow sets how many keys Receive lists from the head of the message
// keyspace per attempt. Size it above the worker count so leased-but-unacked
// messages — whose payloads stay listed until acked — do not crowd out available
// ones. Defaults to DefaultHeadWindow.
func WithQueueHeadWindow(n int) QueueOption {
	return func(q *Queue) { q.headWindow = n }
}

// WithQueueReceiveBackoff sets the empty-queue poll backoff for the blocking Receive:
// the wait starts at low, doubles (jittered) up to high while the queue stays
// empty, and resets on the next claim. Defaults to DefaultMinPollInterval and
// DefaultMaxPollInterval.
func WithQueueReceiveBackoff(low, high time.Duration) QueueOption {
	return func(q *Queue) {
		q.minPoll = low
		q.maxPoll = high
	}
}

// WithQueueClock injects the time source used for lease deadlines and message
// ids, mainly for tests. It should track wall-clock rate, since lease deadlines
// are compared against it on every host. Defaults to time.Now.
func WithQueueClock(now func() time.Time) QueueOption {
	return func(q *Queue) { q.clock = now }
}

// WithMaxReceives caps how many times a message may be delivered before it is
// dead-lettered instead of redelivered forever. A message is delivered at most n
// times; the next claim that would redeliver it (a takeover that observes the
// receive count already at n) moves the message aside to the queue's dead/
// sub-prefix — payload, user attributes, and final receive count retained for
// inspection — and removes it from the live queue. The move is performed by the
// claiming worker (no background sweeper) and is guarded by winning the lease
// first, so exactly one worker performs it under contention.
//
// n <= 0 (the default) disables dead-lettering: a poison message is redelivered
// forever, and the caller can implement its own policy from Message.Receives.
func WithMaxReceives(n int) QueueOption {
	return func(q *Queue) { q.maxReceives = n }
}

// NewQueue returns a Queue that stores its messages under prefix in bucket. The
// prefix is required — there is no default — and the queue owns it wholesale;
// sharding past a single prefix's request-rate ceiling is the caller's
// composition (run N queues over N prefixes). NewQueue returns
// ErrInvalidQueuePrefix if the prefix is empty or would escape its subtree.
//
// Pass any blobster driver (mem, file, s3, gcs, azureblob); the queue works on
// any bucket that advertises ConditionalWrites.
func NewQueue(bucket Bucket, prefix string, opts ...QueueOption) (*Queue, error) {
	if prefix == "" {
		return nil, fmt.Errorf("%w: empty prefix", ErrInvalidQueuePrefix)
	}
	if strings.HasPrefix(prefix, "/") || strings.Contains(prefix, "..") {
		return nil, fmt.Errorf("%w: %q", ErrInvalidQueuePrefix, prefix)
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	q := &Queue{
		bucket:     bucket.Sub(prefix),
		prefix:     prefix,
		lease:      DefaultVisibilityLease,
		headWindow: DefaultHeadWindow,
		minPoll:    DefaultMinPollInterval,
		maxPoll:    DefaultMaxPollInterval,
		clock:      time.Now,
	}
	for _, opt := range opts {
		opt(q)
	}
	if q.lease <= 0 {
		q.lease = DefaultVisibilityLease
	}
	if q.renew <= 0 {
		q.renew = q.lease / 3
	}
	if q.renew <= 0 {
		q.renew = q.lease
	}
	if q.headWindow <= 0 {
		q.headWindow = DefaultHeadWindow
	}
	if q.minPoll <= 0 {
		q.minPoll = DefaultMinPollInterval
	}
	if q.maxPoll < q.minPoll {
		q.maxPoll = q.minPoll
	}
	if q.maxReceives < 0 {
		q.maxReceives = 0
	}
	return q, nil
}

// EnqueueOptions carries per-message options for Queue.Enqueue.
type EnqueueOptions struct {
	// Attributes are user metadata stored on the payload object and surfaced to
	// the receiver via Message.Attributes. They are set once at enqueue and never
	// touched again, so they never clash with the lease state on the separate
	// lease object.
	Attributes map[string]string
}

// Enqueue writes a new message with body r and returns its id. The payload is
// streamed (any size, never buffered) and written create-only, so an enqueue
// never overwrites an existing message. Availability means the payload exists
// with no live lease.
func (q *Queue) Enqueue(ctx context.Context, r io.Reader, opts *EnqueueOptions) (string, error) {
	var attrs map[string]string
	if opts != nil {
		attrs = opts.Attributes
	}
	id := newQueueID(q.clock())
	if err := q.writePayload(ctx, id, r, attrs); err != nil {
		return "", err
	}
	return id, nil
}

func (q *Queue) writePayload(ctx context.Context, id string, r io.Reader, attrs map[string]string) error {
	w, err := q.bucket.NewWriter(ctx, queueMsgPrefix+id, &WriterOptions{
		ContentType:                 "application/octet-stream",
		DisableContentTypeDetection: true,
		Metadata:                    attrs,
	}, IfNotExists)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		return errors.Join(err, w.CloseWithError(err))
	}
	return w.Close()
}

// deadLetter moves a poison message out of the live queue into the dead/
// sub-prefix and removes it. The caller must already hold the lease (leaseVersion
// is the version it won), which makes it the sole mover under contention.
//
// The dead record carries the payload, its user attributes, and the final
// receive count in one self-describing object. The base Copy is server-side but
// cannot set destination metadata (and the receive count lives on the lease
// object, not the payload), so the record is written by streaming the payload
// into it with the count merged in — still no whole-body buffering.
//
// The live message is then removed payload-first, then lease. Payload-first is
// the inverse of Ack's lease-first ordering and is deliberate: the caller holds a
// live lease throughout, so deleting the payload first cannot resurrect the
// message (a live-leased message is skipped by every contender, and once the
// payload is gone it is not listed at all), whereas deleting the lease first would
// re-expose the payload as an available message and redeliver one already
// dead-lettered. A crash before the payload delete is recoverable — payload and
// lease both survive with the count at the threshold, so the next claim re-runs
// the idempotent move once the lease expires. A crash between the two deletes
// leaves an empty-bodied orphan lease that discovery (which lists msg/) never
// revisits: harmless, and it never resurrects the dead-lettered message.
func (q *Queue) deadLetter(ctx context.Context, id string, receives int, leaseVersion string) error {
	msgPath := queueMsgPrefix + id
	leasePath := queueLeasePrefix + id

	attrs, err := q.bucket.Attributes(ctx, msgPath)
	switch {
	case errors.Is(err, ErrNotFound):
		// A previous partial dead-letter already moved the payload; fall through to
		// finish removing the live message.
	case err != nil:
		return err
	default:
		if err := q.writeDeadRecord(ctx, id, attrs.Metadata, receives); err != nil {
			return err
		}
	}

	if err := q.bucket.Delete(ctx, msgPath); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err := q.bucket.Delete(ctx, leasePath, IfMatch(leaseVersion)); err != nil &&
		!errors.Is(err, ErrPreconditionFailed) && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// writeDeadRecord streams the payload into the dead/ record, merging the final
// receive count into the payload's user attributes as the record's metadata. The
// write is unconditional so a retried dead-letter (after a partial earlier
// attempt) simply rewrites identical content.
func (q *Queue) writeDeadRecord(ctx context.Context, id string, attrs map[string]string, receives int) error {
	meta := CloneMetadata(attrs)
	if meta == nil {
		meta = make(map[string]string, 1)
	}
	meta[queueReceivesField] = strconv.Itoa(receives)

	r, err := q.bucket.NewReader(ctx, queueMsgPrefix+id, nil)
	if errors.Is(err, ErrNotFound) {
		return nil // payload vanished under us; a concurrent mover handled it
	}
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := q.bucket.NewWriter(ctx, queueDeadPrefix+id, &WriterOptions{
		ContentType:                 "application/octet-stream",
		DisableContentTypeDetection: true,
		Metadata:                    meta,
	})
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		return errors.Join(err, w.CloseWithError(err))
	}
	return w.Close()
}

// TryReceive makes a single pass: it lists a head window of messages, picks a
// candidate, and tries to claim its lease. It returns a held Message on success,
// ErrNoMessages if nothing was claimable, or a backend error. It mirrors the
// lock's TryAcquire. The returned Message renews its lease on a background
// goroutine bound to an internal context, stopped only by Ack/Nack or lease loss.
func (q *Queue) TryReceive(ctx context.Context) (*Message, error) {
	return q.tryReceive(ctx)
}

// Receive blocks until a message is claimed or ctx is done, polling with
// exponential jittered backoff while the queue is empty. It mirrors the lock's
// Acquire.
func (q *Queue) Receive(ctx context.Context) (*Message, error) {
	backoff := q.minPoll
	for {
		msg, err := q.tryReceive(ctx)
		if err == nil {
			return msg, nil
		}
		if !errors.Is(err, ErrNoMessages) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(queueJitter(backoff)):
		}
		if backoff < q.maxPoll {
			backoff = min(backoff*2, q.maxPoll)
		}
	}
}

func (q *Queue) tryReceive(ctx context.Context) (*Message, error) {
	owner, err := randomOwner()
	if err != nil {
		return nil, err
	}
	objs, _, err := q.bucket.ListPage(ctx, FirstPageToken, q.headWindow, &ListOptions{Prefix: queueMsgPrefix})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, ErrNoMessages
	}

	// Prefer the head (lexically earliest ≈ oldest) but start at a jittered offset
	// within the front window so concurrent workers do not all contend for the
	// single first message. Wrap around so every candidate is still considered.
	start := rand.IntN(min(len(objs), queueJitterWindow))
	for i := range objs {
		obj := objs[(start+i)%len(objs)]
		id, ok := strings.CutPrefix(obj.Key, queueMsgPrefix)
		if !ok || id == "" {
			continue
		}
		msg, err := q.tryClaim(ctx, id, owner)
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
	}
	return nil, ErrNoMessages
}

// tryClaim attempts to acquire the lease for one message id. It returns a held
// Message on success, (nil, nil) when the candidate is not claimable (a live
// lease, a lost claim race, or a payload that vanished under us), or (nil, err)
// on a backend failure. This is the lock's tryAcquire, plus the receives count
// and a payload-existence check.
func (q *Queue) tryClaim(ctx context.Context, id, owner string) (*Message, error) {
	leasePath := queueLeasePrefix + id
	msgPath := queueMsgPrefix + id

	fields, version, err := q.getLease(ctx, leasePath)
	now := q.clock()

	var newVersion string
	var receives int
	switch {
	case errors.Is(err, ErrNotFound):
		receives = 1
		newVersion, err = q.writeLease(ctx, leasePath, owner, now, receives, IfNotExists)
		if errors.Is(err, ErrPreconditionFailed) {
			return nil, nil // lost the create race to another worker
		}
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		if !queueLeaseExpired(fields, now) {
			return nil, nil // a live holder owns it; skip
		}
		prev := queueParseReceives(fields)
		if q.maxReceives > 0 && prev >= q.maxReceives {
			// The message has been delivered its full allowance; dead-letter it
			// instead of redelivering. Win the lease first (CAS on the stale
			// version, receives left at prev since this is not a delivery) so the
			// takeover makes us the sole mover under contention, then move it.
			holdVersion, err := q.writeLease(ctx, leasePath, owner, now, prev, IfMatch(version))
			if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrNotFound) {
				return nil, nil // lost the race; the winner dead-letters it
			}
			if err != nil {
				return nil, err
			}
			if err := q.deadLetter(ctx, id, prev, holdVersion); err != nil {
				return nil, err
			}
			return nil, nil // moved aside, not delivered
		}
		receives = prev + 1
		newVersion, err = q.writeLease(ctx, leasePath, owner, now, receives, IfMatch(version))
		if errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrNotFound) {
			return nil, nil // someone else took over or deleted it first
		}
		if err != nil {
			return nil, err
		}
	}

	// We hold the lease. The payload may have been deleted by a previous owner's
	// ack that raced our claim (its lease-first delete left the lease absent for
	// us to recreate just before it deleted the payload). Verify the payload still
	// exists; if not, drop the orphan lease we just wrote and move on.
	attrs, err := q.bucket.Attributes(ctx, msgPath)
	if errors.Is(err, ErrNotFound) {
		_ = q.bucket.Delete(ctx, leasePath, IfMatch(newVersion))
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return q.startMessage(id, msgPath, leasePath, owner, attrs.Metadata, receives, newVersion, now.Add(q.lease)), nil
}

// getLease reads a lease record's fields and version, returning ErrNotFound if it
// is absent.
func (q *Queue) getLease(ctx context.Context, path string) (map[string]string, string, error) {
	attrs, err := q.bucket.Attributes(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return attrs.Metadata, attrs.Version, nil
}

// writeLease writes a lease record under the given precondition and returns its
// new version. Like the lock, the write path does not return the new version
// token, so it is read back for the next compare-and-swap; we hold the record at
// this point (a create or CAS just succeeded within the lease), so the read
// cannot observe a foreign write.
func (q *Queue) writeLease(ctx context.Context, path, owner string, now time.Time, receives int, precondition Precondition) (string, error) {
	opts := &WriterOptions{Metadata: q.leaseRecord(owner, now, receives), DisableContentTypeDetection: true}
	if err := q.bucket.WriteAll(ctx, path, nil, opts, precondition); err != nil {
		return "", err
	}
	attrs, err := q.bucket.Attributes(ctx, path)
	if err != nil {
		return "", err
	}
	return attrs.Version, nil
}

func (q *Queue) leaseRecord(owner string, now time.Time, receives int) map[string]string {
	return map[string]string{
		queueOwnerField:    owner,
		queueLeaseField:    now.Add(q.lease).Format(time.RFC3339Nano),
		queueReceivesField: strconv.Itoa(receives),
	}
}

// queueLeaseExpired reports whether a lease record's deadline is at or before now.
// A record with a missing or unparseable lease is treated as expired so a
// malformed record can always be recovered, mirroring the lock.
func queueLeaseExpired(fields map[string]string, now time.Time) bool {
	raw, ok := fields[queueLeaseField]
	if !ok {
		return true
	}
	until, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return true
	}
	return !now.Before(until)
}

// queueParseReceives reads the receives count from a lease record, treating a
// missing or malformed value as zero so a takeover still increments to a sane
// count.
func queueParseReceives(fields map[string]string) int {
	n, err := strconv.Atoi(fields[queueReceivesField])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func queueJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// Full jitter in [d, 2d) to spread contenders off the hot keyspace.
	return d + time.Duration(rand.Int64N(int64(d)))
}

// newQueueID returns a message id that sorts lexicographically by creation time:
// an epoch-millisecond timestamp followed by a random UUID, joined by a dash. The
// timestamp is the load-bearing part — lexical order tracks time, which gives the
// queue its approximate-FIFO discovery order; the UUID only breaks ties within a
// millisecond, keeping ids unique with no hot shared counter. It is width-padded
// to 13 digits, the natural width of epoch milliseconds (and a constant from year
// 2001 to 2286), so the decimal string sorts in the same order as the number. The
// clock is injected so tests can drive ordering deterministically.
func newQueueID(now time.Time) string {
	return fmt.Sprintf("%013d-%s", now.UnixMilli(), uuid.New())
}
