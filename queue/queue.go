// Package queue is a work queue built solely on blob storage: competing
// consumers, at-least-once delivery, and approximate FIFO, with no broker, no
// database, and no coordinator beyond the bucket's conditional-write primitive.
//
// It is the next coordination primitive after the lease lock and reuses the
// lock's algorithm per message. Each message is two objects under a
// caller-supplied prefix the queue owns wholesale:
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
// liveness-not-safety contract the lock documents. Ordering is approximate:
// ids are ULID-style (millisecond time plus randomness, lexically sortable),
// and workers prefer the lexically-earliest available message, but concurrency,
// clock skew, and redelivery make it best-effort only.
package queue

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/yolocs/blobster"
)

// Queue sentinel errors, matchable with errors.Is.
var (
	// ErrNoMessages is returned by TryReceive when no message is available to
	// claim — the queue is empty, or every listed message is currently leased.
	ErrNoMessages = errors.New("blobster/queue: no messages available")
	// ErrMessageLost reports, via a Message's Err, that its lease could not be
	// renewed and the message is no longer held; another worker may redeliver it.
	ErrMessageLost = errors.New("blobster/queue: message lost")
	// ErrInvalidQueuePrefix is returned by New when the prefix is empty or would
	// escape its subtree.
	ErrInvalidQueuePrefix = errors.New("blobster/queue: invalid queue prefix")
)

// Default lease timing. The lease is second-scale for the same reason the lock's
// is: GCS throttles writes to ~1/sec per object name, and the lease record is a
// single hot object, so sub-second leases are not portable across backends.
const (
	DefaultVisibilityLease = 15 * time.Second
	DefaultHeadWindow      = 256
	DefaultMinPollInterval = 250 * time.Millisecond
	DefaultMaxPollInterval = 5 * time.Second
)

// Sub-prefixes, relative to the queue's owned prefix, that separate the immutable
// payloads from the mutating lease records.
const (
	msgPrefix   = "msg/"
	leasePrefix = "lease/"
)

// Lease record field names, stored as the lease object's user metadata. They are
// lowercase identifiers that round-trip verbatim on every backend (including the
// escaping azureblob driver), exactly like the lock's record fields.
const (
	leaseOwnerField    = "owner"
	leaseLeaseField    = "lease"
	leaseReceivesField = "receives"
)

// jitterWindow bounds how far from the head of the listing a claim attempt may
// start. Workers iterate the head window in lexical (≈FIFO) order but begin at a
// random offset within this front window, so they prefer the earliest messages
// without all stampeding the single lexically-first one.
const jitterWindow = 16

// Queue is a blob-backed work queue rooted at one prefix of a Bucket. It is safe
// for concurrent use; construct one per prefix with New and share it across
// workers. The bucket must advertise ConditionalWrites.
type Queue struct {
	bucket     blobster.Bucket // rooted at the queue's prefix via Sub
	prefix     string          // the caller-supplied prefix, normalized with a trailing slash
	lease      time.Duration
	renew      time.Duration
	headWindow int
	minPoll    time.Duration
	maxPoll    time.Duration
	clock      func() time.Time
}

// Option configures a Queue.
type Option func(*Queue)

// WithVisibilityLease sets how long a claimed message stays invisible to other
// workers before its lease must be renewed. A crashed worker's message becomes
// redeliverable this long after its last successful renew. Defaults to
// DefaultVisibilityLease.
func WithVisibilityLease(d time.Duration) Option {
	return func(q *Queue) { q.lease = d }
}

// WithRenewInterval sets how often a held message renews its lease in the
// background. It must be comfortably shorter than the lease to absorb transient
// backend errors and clock skew. Defaults to one third of the lease.
func WithRenewInterval(d time.Duration) Option {
	return func(q *Queue) { q.renew = d }
}

// WithHeadWindow sets how many keys Receive lists from the head of the message
// keyspace per attempt. Size it above the worker count so leased-but-unacked
// messages — whose payloads stay listed until acked — do not crowd out available
// ones. Defaults to DefaultHeadWindow.
func WithHeadWindow(n int) Option {
	return func(q *Queue) { q.headWindow = n }
}

// WithReceiveBackoff sets the empty-queue poll backoff for the blocking Receive:
// the wait starts at min, doubles (jittered) up to max while the queue stays
// empty, and resets on the next claim. Defaults to DefaultMinPollInterval and
// DefaultMaxPollInterval.
func WithReceiveBackoff(low, high time.Duration) Option {
	return func(q *Queue) {
		q.minPoll = low
		q.maxPoll = high
	}
}

// WithClock injects the time source used for lease deadlines and message ids,
// mainly for tests. It should track wall-clock rate, since lease deadlines are
// compared against it on every host. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.clock = now }
}

// New returns a Queue that stores its messages under prefix in bucket. The prefix
// is required — there is no default — and the queue owns it wholesale; sharding
// past a single prefix's request-rate ceiling is the caller's composition (run N
// queues over N prefixes). New returns ErrInvalidQueuePrefix if the prefix is
// empty or would escape its subtree.
//
// Pass any blobster driver (mem, file, s3, gcs, azureblob); the queue works on
// any bucket that advertises ConditionalWrites.
func New(bucket blobster.Bucket, prefix string, opts ...Option) (*Queue, error) {
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
	return q, nil
}

// EnqueueOptions carries per-message options for Enqueue.
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
	// Retry on the astronomically unlikely id collision rather than surfacing it.
	for attempt := 0; attempt < 3; attempt++ {
		id, err := newID(q.clock())
		if err != nil {
			return "", err
		}
		err = q.writePayload(ctx, id, r, attrs)
		if errors.Is(err, blobster.ErrPreconditionFailed) {
			continue
		}
		if err != nil {
			return "", err
		}
		return id, nil
	}
	return "", fmt.Errorf("blobster/queue: could not allocate a unique message id")
}

func (q *Queue) writePayload(ctx context.Context, id string, r io.Reader, attrs map[string]string) error {
	w, err := q.bucket.NewWriter(ctx, msgPrefix+id, &blobster.WriterOptions{
		ContentType:                 "application/octet-stream",
		DisableContentTypeDetection: true,
		Metadata:                    attrs,
	}, blobster.IfNotExists)
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
		case <-time.After(jitter(backoff)):
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
	objs, _, err := q.bucket.ListPage(ctx, blobster.FirstPageToken, q.headWindow, &blobster.ListOptions{Prefix: msgPrefix})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, ErrNoMessages
	}

	// Prefer the head (lexically earliest ≈ oldest) but start at a jittered offset
	// within the front window so concurrent workers do not all contend for the
	// single first message. Wrap around so every candidate is still considered.
	start := rand.IntN(min(len(objs), jitterWindow))
	for i := range objs {
		obj := objs[(start+i)%len(objs)]
		id, ok := strings.CutPrefix(obj.Key, msgPrefix)
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
	leasePath := leasePrefix + id
	msgPath := msgPrefix + id

	fields, version, err := q.getRecord(ctx, leasePath)
	now := q.clock()

	var newVersion string
	var receives int
	switch {
	case errors.Is(err, blobster.ErrNotFound):
		receives = 1
		newVersion, err = q.writeLease(ctx, leasePath, owner, now, receives, blobster.IfNotExists)
		if errors.Is(err, blobster.ErrPreconditionFailed) {
			return nil, nil // lost the create race to another worker
		}
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		if !leaseExpired(fields, now) {
			return nil, nil // a live holder owns it; skip
		}
		receives = parseReceives(fields) + 1
		newVersion, err = q.writeLease(ctx, leasePath, owner, now, receives, blobster.IfMatch(version))
		if errors.Is(err, blobster.ErrPreconditionFailed) || errors.Is(err, blobster.ErrNotFound) {
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
	if errors.Is(err, blobster.ErrNotFound) {
		_ = q.bucket.Delete(ctx, leasePath, blobster.IfMatch(newVersion))
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return q.startMessage(id, msgPath, leasePath, owner, attrs.Metadata, receives, newVersion, now.Add(q.lease)), nil
}

// getRecord reads a lease record's fields and version, returning ErrNotFound if
// it is absent.
func (q *Queue) getRecord(ctx context.Context, path string) (map[string]string, string, error) {
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
func (q *Queue) writeLease(ctx context.Context, path, owner string, now time.Time, receives int, precondition blobster.Precondition) (string, error) {
	opts := &blobster.WriterOptions{Metadata: q.record(owner, now, receives), DisableContentTypeDetection: true}
	if err := q.bucket.WriteAll(ctx, path, nil, opts, precondition); err != nil {
		return "", err
	}
	attrs, err := q.bucket.Attributes(ctx, path)
	if err != nil {
		return "", err
	}
	return attrs.Version, nil
}

func (q *Queue) record(owner string, now time.Time, receives int) map[string]string {
	return map[string]string{
		leaseOwnerField:    owner,
		leaseLeaseField:    now.Add(q.lease).Format(time.RFC3339Nano),
		leaseReceivesField: strconv.Itoa(receives),
	}
}

// leaseExpired reports whether a lease record's deadline is at or before now. A
// record with a missing or unparseable lease is treated as expired so a malformed
// record can always be recovered, mirroring the lock.
func leaseExpired(fields map[string]string, now time.Time) bool {
	raw, ok := fields[leaseLeaseField]
	if !ok {
		return true
	}
	until, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return true
	}
	return !now.Before(until)
}

// parseReceives reads the receives count from a lease record, treating a missing
// or malformed value as zero so a takeover still increments to a sane count.
func parseReceives(fields map[string]string) int {
	n, err := strconv.Atoi(fields[leaseReceivesField])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// Full jitter in [d, 2d) to spread contenders off the hot keyspace.
	return d + time.Duration(rand.Int64N(int64(d)))
}

func randomOwner() (string, error) {
	var buf [16]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
