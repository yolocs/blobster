package blobster_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/mem"
)

// This file reuses the shared test helpers defined in locker_test.go and
// queue_test.go (eachBucket, manualClock, mustNewQueue, enqueueString).

func tailIDs(msgs []*blobster.TailMessage) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID()
	}
	return ids
}

// TestQueueTailPagesInOrderReadOnly verifies forward, read-only iteration: the
// tail surfaces messages in id order across resumed pages, the returned cursor
// resumes exactly, an exhausted tail leaves the cursor unchanged, and nothing
// is ever written to the source's lease keyspace.
func TestQueueTailPagesInOrderReadOnly(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "events/", blobster.WithQueueClock(clock.Now))

		var ids []string
		for i := range 5 {
			ids = append(ids, enqueueString(t, q, fmt.Sprintf("body-%d", i), map[string]string{"seq": fmt.Sprint(i)}))
			clock.Advance(time.Millisecond)
		}

		tail := q.Tail(blobster.WithTailLag(0))

		page1, c1, err := tail.Page(ctx, "", 2)
		if err != nil {
			t.Fatalf("page 1: %v", err)
		}
		if got, want := tailIDs(page1), ids[:2]; !equalStrings(got, want) {
			t.Fatalf("page 1 ids = %v, want %v", got, want)
		}
		if c1 != ids[1] {
			t.Fatalf("cursor after page 1 = %q, want %q", c1, ids[1])
		}

		page2, c2, err := tail.Page(ctx, c1, 2)
		if err != nil {
			t.Fatalf("page 2: %v", err)
		}
		if got, want := tailIDs(page2), ids[2:4]; !equalStrings(got, want) {
			t.Fatalf("page 2 ids = %v, want %v", got, want)
		}

		page3, c3, err := tail.Page(ctx, c2, 2)
		if err != nil {
			t.Fatalf("page 3: %v", err)
		}
		if got, want := tailIDs(page3), ids[4:]; !equalStrings(got, want) {
			t.Fatalf("page 3 ids = %v, want %v", got, want)
		}

		// Exhausted: nothing surfaced, cursor unchanged.
		page4, c4, err := tail.Page(ctx, c3, 2)
		if err != nil {
			t.Fatalf("page 4: %v", err)
		}
		if len(page4) != 0 {
			t.Fatalf("page 4 surfaced %v past the end", tailIDs(page4))
		}
		if c4 != c3 {
			t.Fatalf("cursor moved on an empty page: %q -> %q", c3, c4)
		}

		// Message surface: id, streamed payload, attributes.
		m := page1[0]
		attrs, err := m.Attributes(ctx)
		if err != nil {
			t.Fatalf("Attributes: %v", err)
		}
		if got := attrs["seq"]; got != "0" {
			t.Errorf("Attributes[seq] = %q, want 0", got)
		}
		r, err := m.Body(ctx)
		if err != nil {
			t.Fatalf("Body: %v", err)
		}
		var sb strings.Builder
		if _, err := r.WriteTo(&sb); err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
		if sb.String() != "body-0" {
			t.Errorf("body = %q, want body-0", sb.String())
		}

		// Read-only: tailing wrote nothing into the lease (or any other) keyspace.
		leases, _, err := bucket.ListPage(ctx, blobster.FirstPageToken, 10, &blobster.ListOptions{Prefix: "events/lease/"})
		if err != nil {
			t.Fatalf("list leases: %v", err)
		}
		if len(leases) != 0 {
			t.Fatalf("tailing wrote %d lease records", len(leases))
		}
	})
}

// TestQueueTailLagWithholdsRecent verifies the cursor lag: a message stamped
// within the lag window is not yet surfaced, and appears once the clock moves
// past id-time + lag.
func TestQueueTailLagWithholdsRecent(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "events/", blobster.WithQueueClock(clock.Now))
		id := enqueueString(t, q, "fresh", nil)

		tail := q.Tail(blobster.WithTailLag(5 * time.Second))
		msgs, next, err := tail.Page(ctx, "", 10)
		if err != nil {
			t.Fatalf("Page within lag: %v", err)
		}
		if len(msgs) != 0 {
			t.Fatalf("message surfaced inside the lag window: %v", tailIDs(msgs))
		}
		if next != "" {
			t.Fatalf("cursor advanced inside the lag window: %q", next)
		}

		clock.Advance(5*time.Second + time.Millisecond)
		msgs, next, err = tail.Page(ctx, "", 10)
		if err != nil {
			t.Fatalf("Page past lag: %v", err)
		}
		if len(msgs) != 1 || msgs[0].ID() != id {
			t.Fatalf("Page past lag = %v, want [%s]", tailIDs(msgs), id)
		}
		if next != id {
			t.Fatalf("cursor = %q, want %q", next, id)
		}
	})
}

// TestQueueTailLateCommitNotSkipped pins the one real late-arrival hole the lag
// closes: a message whose id was stamped earlier but whose payload committed
// later than a newer message must still be surfaced, in id order, because the
// tailer never advances its cursor into the lag window.
func TestQueueTailLateCommitNotSkipped(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "events/", blobster.WithQueueClock(clock.Now))
		tail := q.Tail(blobster.WithTailLag(5 * time.Second))

		base := clock.Now()
		idSlow := fmt.Sprintf("%013d-slow", base.UnixMilli())                  // stamped first, commits late
		idFast := fmt.Sprintf("%013d-fast", base.Add(time.Second).UnixMilli()) // stamped second, commits first

		clock.Advance(time.Second)
		if _, err := q.EnqueueWithID(ctx, idFast, strings.NewReader("fast"), nil); err != nil {
			t.Fatalf("enqueue fast: %v", err)
		}

		// The fast message is committed and listed, but the lag window holds the
		// cursor back, so it cannot be surfaced past the not-yet-committed slow one.
		clock.Advance(3 * time.Second) // now = base+4s; slow un-lags at +5s, fast at +6s
		msgs, next, err := tail.Page(ctx, "", 10)
		if err != nil {
			t.Fatalf("Page inside lag: %v", err)
		}
		if len(msgs) != 0 || next != "" {
			t.Fatalf("cursor advanced past the uncommitted slow message: %v, cursor %q", tailIDs(msgs), next)
		}

		// The slow payload commits late — within the lag window.
		if _, err := q.EnqueueWithID(ctx, idSlow, strings.NewReader("slow"), nil); err != nil {
			t.Fatalf("enqueue slow: %v", err)
		}

		clock.Advance(2 * time.Second) // now = base+6s; both beyond the lag
		msgs, next, err = tail.Page(ctx, "", 10)
		if err != nil {
			t.Fatalf("Page past lag: %v", err)
		}
		if got, want := tailIDs(msgs), []string{idSlow, idFast}; !equalStrings(got, want) {
			t.Fatalf("Page = %v, want %v (late-committed message skipped?)", got, want)
		}
		if next != idFast {
			t.Fatalf("cursor = %q, want %q", next, idFast)
		}
	})
}

// TestQueueTailUnsortableIDNotLagGated verifies the documented behavior for
// caller-supplied ids outside the time-sortable shape: they carry no timestamp,
// so the lag gate does not apply and they surface as soon as they are listed.
func TestQueueTailUnsortableIDNotLagGated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	clock := newManualClock()
	q := mustNewQueue(t, mem.New(), "events/", blobster.WithQueueClock(clock.Now))
	if _, err := q.EnqueueWithID(ctx, "custom", strings.NewReader("x"), nil); err != nil {
		t.Fatalf("EnqueueWithID: %v", err)
	}

	tail := q.Tail(blobster.WithTailLag(5 * time.Second))
	msgs, next, err := tail.Page(ctx, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID() != "custom" {
		t.Fatalf("Page = %v, want [custom]", tailIDs(msgs))
	}
	if next != "custom" {
		t.Fatalf("cursor = %q, want custom", next)
	}
}

// TestQueueTailEmptyQueue verifies tailing an empty queue is a clean no-op.
func TestQueueTailEmptyQueue(t *testing.T) {
	t.Parallel()
	q := mustNewQueue(t, mem.New(), "events/")
	msgs, next, err := q.Tail().Page(t.Context(), "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(msgs) != 0 || next != "" {
		t.Fatalf("Page on empty queue = %v, cursor %q", tailIDs(msgs), next)
	}
}

// TestQueueTailVanishedMessage verifies the surface of a tailed message whose
// payload was removed (acked or trimmed) after listing: reads report
// ErrNotFound rather than failing opaquely.
func TestQueueTailVanishedMessage(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := mem.New()
	q := mustNewQueue(t, b, "events/")
	id := enqueueString(t, q, "doomed", nil)

	msgs, _, err := q.Tail(blobster.WithTailLag(0)).Page(ctx, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Page = %v, want one message", tailIDs(msgs))
	}
	if err := b.Delete(ctx, "events/msg/"+id); err != nil {
		t.Fatalf("delete payload: %v", err)
	}
	if _, err := msgs[0].Attributes(ctx); !errors.Is(err, blobster.ErrNotFound) {
		t.Fatalf("Attributes after removal: got %v, want ErrNotFound", err)
	}
	if _, err := msgs[0].ReadAll(ctx); !errors.Is(err, blobster.ErrNotFound) {
		t.Fatalf("ReadAll after removal: got %v, want ErrNotFound", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
