package blobster

import (
	"context"
	"strings"
	"time"
)

// DefaultTailLag is the default cursor lag of a Tail: how far behind the clock
// the tail's visible head trails, so a message whose id was stamped at enqueue
// but whose payload committed late is not skipped by an already-advanced
// cursor. It must exceed worst-case payload write latency plus producer clock
// skew.
const DefaultTailLag = 5 * time.Second

// tailListPageSize is the backend page size used while scanning toward the
// cursor; it is independent of the caller's pageSize, which bounds only what a
// Page call surfaces.
const tailListPageSize = 1000

// Tail is a read-only, forward view over a queue's message log: it surfaces
// messages whose ids sort after a caller-supplied cursor without taking a
// lease, acking, or writing anything to the queue's bucket. Where Receive is a
// competing consumer, Tail is a log tailer — the shape a fan-out replication
// follower needs, since it can read the leader's bucket but cannot write lease
// or ack records into it.
//
// The cursor is the id of the last message consumed and is the caller's to
// persist (the replication Watcher stores it in its own bucket); Tail itself is
// stateless and safe for concurrent use. Ordering and the lag gate rely on
// time-sortable ids; a caller-supplied id outside that shape (EnqueueWithID)
// surfaces at its arbitrary lexical position with no lag protection.
type Tail struct {
	queue *Queue
	lag   time.Duration
}

// TailOption configures a Tail.
type TailOption func(*Tail)

// WithTailLag sets the cursor lag: a message whose id is stamped within this
// window of the queue clock's now is not yet surfaced, so an earlier-stamped
// payload that commits late (bounded by payload write latency plus producer
// clock skew) cannot be skipped by a cursor that already advanced past it.
// Defaults to DefaultTailLag; zero disables the gate (only safe when a single
// producer enqueues and commits in order, as tests do).
func WithTailLag(d time.Duration) TailOption {
	return func(t *Tail) {
		if d < 0 {
			d = 0
		}
		t.lag = d
	}
}

// Tail returns a read-only tail view over the queue's message log. The view
// shares the queue's bucket and clock but never writes: it is safe over a
// bucket the caller can only read.
func (q *Queue) Tail(opts ...TailOption) *Tail {
	t := &Tail{queue: q, lag: DefaultTailLag}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Page returns up to pageSize messages whose ids sort after cursor and are
// stamped at or before the lag-trimmed head (now − lag), in id order, plus the
// cursor to resume from — the id of the last message returned, or the input
// cursor unchanged when nothing new is visible. An empty cursor starts from the
// beginning of the log. pageSize <= 0 defaults to DefaultHeadWindow.
//
// Page scans the message keyspace from its head each call, skipping keys at or
// before the cursor, so its cost tracks the retained log length — pair the
// source queue with WithRetention/Trim so the log (and the scan) stays bounded.
func (t *Tail) Page(ctx context.Context, cursor string, pageSize int) ([]*TailMessage, string, error) {
	if pageSize <= 0 {
		pageSize = DefaultHeadWindow
	}
	now := t.queue.clock()
	next := cursor
	var out []*TailMessage
	token := FirstPageToken
	for {
		objs, nextToken, err := t.queue.bucket.ListPage(ctx, token, tailListPageSize, &ListOptions{Prefix: queueMsgPrefix})
		if err != nil {
			return nil, "", err
		}
		for _, obj := range objs {
			id, ok := strings.CutPrefix(obj.Key, queueMsgPrefix)
			if !ok || id == "" {
				continue
			}
			if id <= cursor {
				continue
			}
			if ts, ok := queueIDTime(id); ok && now.Before(ts.Add(t.lag)) {
				// Inside the lag window. Time-sortable ids are lexically ordered, so
				// every later one is stamped later still: stop here rather than
				// surface a newer id past an earlier-stamped payload that may not
				// have committed yet.
				return out, next, nil
			}
			out = append(out, &TailMessage{queue: t.queue, id: id})
			next = id
			if len(out) >= pageSize {
				return out, next, nil
			}
		}
		if len(nextToken) == 0 {
			return out, next, nil
		}
		token = nextToken
	}
}

// TailMessage is a message surfaced by a Tail: the received Message's surface
// minus the lease — no Ack, Nack, lost-lease signal, or background renewer,
// because tailing claims nothing. Reads go straight to the immutable payload,
// so they remain valid for as long as the payload exists (until it is acked
// away by a competing consumer or trimmed by retention).
type TailMessage struct {
	queue *Queue
	id    string
}

// ID returns the message's id — on a tailed queue, the cursor value to resume
// after.
func (m *TailMessage) ID() string { return m.id }

// Attributes reads the user metadata stored with the payload at enqueue. It is
// a read against the payload object, so it returns ErrNotFound once the
// message is acked or trimmed away.
func (m *TailMessage) Attributes(ctx context.Context) (map[string]string, error) {
	attrs, err := m.queue.bucket.Attributes(ctx, queueMsgPrefix+m.id)
	if err != nil {
		return nil, err
	}
	return CloneMetadata(attrs.Metadata), nil
}

// Body opens a streaming reader over the message payload; the caller is
// responsible for closing it.
func (m *TailMessage) Body(ctx context.Context) (Reader, error) {
	return m.queue.bucket.NewReader(ctx, queueMsgPrefix+m.id, nil)
}

// ReadAll reads the entire payload into memory. Prefer Body for large payloads.
func (m *TailMessage) ReadAll(ctx context.Context) ([]byte, error) {
	return m.queue.bucket.ReadAll(ctx, queueMsgPrefix+m.id)
}
