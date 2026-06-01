package blobster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/mem"
)

// This file reuses the shared test helpers defined in locker_test.go
// (bucketFactory, eachBucket, manualClock/newManualClock, faultBucket), since the
// queue lives in the same root package as the lock and exercises the same real
// mem/file drivers.

func mustNewQueue(t *testing.T, bucket blobster.Bucket, prefix string, opts ...blobster.QueueOption) *blobster.Queue {
	t.Helper()
	q, err := blobster.NewQueue(bucket, prefix, opts...)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	return q
}

func enqueueString(t *testing.T, q *blobster.Queue, body string, attrs map[string]string) string {
	t.Helper()
	id, err := q.Enqueue(t.Context(), bytes.NewReader([]byte(body)), &blobster.EnqueueOptions{Attributes: attrs})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

func TestQueueEnqueueReceiveAck(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/")

		id := enqueueString(t, q, "hello", map[string]string{"kind": "greeting"})

		msg, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("TryReceive: %v", err)
		}
		if msg.ID() != id {
			t.Errorf("ID = %q, want %q", msg.ID(), id)
		}
		if got := msg.Receives(); got != 1 {
			t.Errorf("Receives = %d, want 1", got)
		}
		if got := msg.Attributes()["kind"]; got != "greeting" {
			t.Errorf("Attributes[kind] = %q, want greeting", got)
		}
		body, err := msg.ReadAll(ctx)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(body) != "hello" {
			t.Errorf("body = %q, want hello", body)
		}

		if err := msg.Ack(ctx); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		// After ack, both objects are gone and the queue is empty.
		if ok, _ := bucket.Exists(ctx, "jobs/lease/"+id); ok {
			t.Error("lease record still exists after ack")
		}
		if ok, _ := bucket.Exists(ctx, "jobs/msg/"+id); ok {
			t.Error("payload still exists after ack")
		}
		if _, err := q.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
			t.Fatalf("TryReceive on empty: got %v, want ErrNoMessages", err)
		}
	})
}

func TestQueueTryReceiveEmpty(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		q := mustNewQueue(t, bucket, "jobs/")
		if _, err := q.TryReceive(t.Context()); !errors.Is(err, blobster.ErrNoMessages) {
			t.Fatalf("got %v, want ErrNoMessages", err)
		}
	})
}

func TestQueueInvalidPrefix(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{"", "/abs/", "a/../b/"} {
		if _, err := blobster.NewQueue(mem.New(), prefix); !errors.Is(err, blobster.ErrInvalidQueuePrefix) {
			t.Errorf("NewQueue(%q): got %v, want ErrInvalidQueuePrefix", prefix, err)
		}
	}
	// A prefix without a trailing slash is normalized, not rejected.
	if _, err := blobster.NewQueue(mem.New(), "jobs"); err != nil {
		t.Errorf("NewQueue(\"jobs\"): unexpected error %v", err)
	}
}

func TestQueueConcurrentReceiveSingleWinner(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/")
		enqueueString(t, q, "only-one", nil)

		const contenders = 16
		var (
			wg      sync.WaitGroup
			winners atomic.Int64
			empty   atomic.Int64
			start   = make(chan struct{})
		)
		var winner atomic.Pointer[blobster.Message]
		for range contenders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				msg, err := q.TryReceive(ctx)
				switch {
				case err == nil:
					winners.Add(1)
					winner.Store(msg)
				case errors.Is(err, blobster.ErrNoMessages):
					empty.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		close(start)
		wg.Wait()

		if got := winners.Load(); got != 1 {
			t.Fatalf("winners = %d, want exactly 1", got)
		}
		if got := empty.Load(); got != contenders-1 {
			t.Fatalf("empty = %d, want %d", got, contenders-1)
		}
		if w := winner.Load(); w != nil {
			if err := w.Ack(ctx); err != nil {
				t.Fatalf("ack winner: %v", err)
			}
		}
	})
}

func TestQueueTakeoverIncrementsReceives(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newManualClock()
		// Renew far in the future so a holder never self-renews; expiry is driven
		// purely by the clock.
		q := mustNewQueue(t, bucket, "jobs/",
			blobster.WithQueueClock(clock.Now),
			blobster.WithVisibilityLease(30*time.Second),
			blobster.WithQueueRenewInterval(time.Hour),
		)
		enqueueString(t, q, "work", nil)

		first, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("first receive: %v", err)
		}
		if first.Receives() != 1 {
			t.Fatalf("first Receives = %d, want 1", first.Receives())
		}

		// Within the lease: not redeliverable.
		if _, err := q.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
			t.Fatalf("receive before expiry: got %v, want ErrNoMessages", err)
		}

		clock.Advance(31 * time.Second)
		second, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("takeover receive: %v", err)
		}
		if second.ID() != first.ID() {
			t.Fatalf("takeover delivered different message: %q vs %q", second.ID(), first.ID())
		}
		if second.Receives() != 2 {
			t.Fatalf("second Receives = %d, want 2", second.Receives())
		}

		clock.Advance(31 * time.Second)
		third, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("third receive: %v", err)
		}
		if third.Receives() != 3 {
			t.Fatalf("third Receives = %d, want 3", third.Receives())
		}

		if err := third.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})
}

func TestQueueNackRedeliversPreservingReceives(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/", blobster.WithQueueRenewInterval(time.Hour))
		id := enqueueString(t, q, "retry-me", nil)

		first, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("first receive: %v", err)
		}
		if err := first.Nack(ctx); err != nil {
			t.Fatalf("nack: %v", err)
		}

		// Nack expires the lease in place, so the message is immediately
		// redeliverable with the receive count preserved (not reset).
		second, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("receive after nack: %v", err)
		}
		if second.ID() != id {
			t.Fatalf("redelivered different message: %q, want %q", second.ID(), id)
		}
		if second.Receives() != 2 {
			t.Fatalf("Receives after nack = %d, want 2", second.Receives())
		}
		if err := second.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})
}

func TestQueueAckAfterTakeoverIsNoOp(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "jobs/",
			blobster.WithQueueClock(clock.Now),
			blobster.WithVisibilityLease(30*time.Second),
			blobster.WithQueueRenewInterval(time.Hour),
		)
		id := enqueueString(t, q, "work", nil)

		first, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("first receive: %v", err)
		}

		// first "crashes" (never renews); its lease expires and a successor claims.
		clock.Advance(31 * time.Second)
		second, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("takeover receive: %v", err)
		}

		// The zombie's Ack must not delete the successor's lease or payload.
		if err := first.Ack(ctx); err != nil {
			t.Fatalf("zombie ack: %v", err)
		}
		if ok, _ := bucket.Exists(ctx, "jobs/msg/"+id); !ok {
			t.Fatal("zombie ack deleted the payload out from under the successor")
		}
		if ok, _ := bucket.Exists(ctx, "jobs/lease/"+id); !ok {
			t.Fatal("zombie ack deleted the successor's lease")
		}

		// The successor still owns it and can ack cleanly.
		if err := second.Ack(ctx); err != nil {
			t.Fatalf("successor ack: %v", err)
		}
		if ok, _ := bucket.Exists(ctx, "jobs/lease/"+id); ok {
			t.Error("lease record still exists after successor ack")
		}
		if ok, _ := bucket.Exists(ctx, "jobs/msg/"+id); ok {
			t.Error("payload still exists after successor ack")
		}
	})
}

func TestQueueLostNotification(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		// Freeze the clock so the only way to lose the message is the renew CAS
		// failing because the lease record changed under us.
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "jobs/",
			blobster.WithQueueClock(clock.Now),
			blobster.WithVisibilityLease(30*time.Second),
			blobster.WithQueueRenewInterval(10*time.Millisecond),
		)
		id := enqueueString(t, q, "work", nil)

		msg, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}

		// Forcibly overwrite the lease record out of band, bumping its version so
		// the holder's next renew CAS fails — a stand-in for a successful takeover.
		if err := bucket.WriteAll(ctx, "jobs/lease/"+id, []byte("x"), &blobster.WriterOptions{DisableContentTypeDetection: true}); err != nil {
			t.Fatalf("out-of-band overwrite: %v", err)
		}

		select {
		case <-msg.Done():
			if !errors.Is(msg.Err(), blobster.ErrMessageLost) {
				t.Fatalf("Err() = %v, want ErrMessageLost", msg.Err())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("lost notification not delivered")
		}
	})
}

func TestQueueRenewKeepsMessageHeld(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		// Freeze the clock so the lease can never lapse from wall-clock timing;
		// prove renewal by observing the lease record's version advance while the
		// message stays held and unlost.
		clock := newManualClock()
		q := mustNewQueue(t, bucket, "jobs/",
			blobster.WithQueueClock(clock.Now),
			blobster.WithVisibilityLease(30*time.Second),
			blobster.WithQueueRenewInterval(20*time.Millisecond),
		)
		id := enqueueString(t, q, "work", nil)

		msg, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		defer msg.Ack(ctx)

		path := "jobs/lease/" + id
		attrs, err := bucket.Attributes(ctx, path)
		if err != nil {
			t.Fatalf("read initial version: %v", err)
		}
		v0 := attrs.Version

		deadline := time.Now().Add(2 * time.Second)
		renewed := false
		for time.Now().Before(deadline) {
			select {
			case <-msg.Done():
				t.Fatalf("message unexpectedly lost: %v", msg.Err())
			default:
			}
			if a, err := bucket.Attributes(ctx, path); err == nil && a.Version != v0 {
				renewed = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !renewed {
			t.Fatal("background renewer did not extend the lease")
		}
	})
}

func TestQueueRenewSurvivesTransientError(t *testing.T) {
	t.Parallel()
	clock := newManualClock()
	fb := &faultBucket{Bucket: mem.New()}
	q := mustNewQueue(t, fb, "jobs/",
		blobster.WithQueueClock(clock.Now),
		blobster.WithVisibilityLease(30*time.Second),
		blobster.WithQueueRenewInterval(10*time.Millisecond),
	)
	ctx := t.Context()
	id := enqueueString(t, q, "work", nil)

	msg, err := q.TryReceive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	defer msg.Ack(ctx)

	// Fail exactly the first renew write, then recover. The frozen clock means the
	// lease cannot lapse meanwhile, so the message must stay held and a later renew
	// must succeed (version advances).
	var calls atomic.Int64
	fb.setWriteAllHook(func(string) error {
		if calls.Add(1) == 1 {
			return errors.New("transient backend error")
		}
		return nil
	})

	path := "jobs/lease/" + id
	attrs, err := fb.Attributes(ctx, path)
	if err != nil {
		t.Fatalf("initial version: %v", err)
	}
	v0 := attrs.Version

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-msg.Done():
			t.Fatalf("message lost on transient error: %v", msg.Err())
		default:
		}
		if a, err := fb.Attributes(ctx, path); err == nil && a.Version != v0 {
			return // a renew succeeded after the injected failure
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("renewer did not recover from a transient error")
}

func TestQueueReceiveBlocksUntilEnqueued(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/", blobster.WithReceiveBackoff(10*time.Millisecond, 20*time.Millisecond))

		received := make(chan *blobster.Message, 1)
		go func() {
			msg, err := q.Receive(ctx)
			if err != nil {
				t.Errorf("Receive: %v", err)
				received <- nil
				return
			}
			received <- msg
		}()

		select {
		case <-received:
			t.Fatal("Receive returned on an empty queue")
		case <-time.After(50 * time.Millisecond):
		}

		enqueueString(t, q, "late", nil)

		select {
		case msg := <-received:
			if msg == nil {
				t.Fatal("blocking receive failed")
			}
			if err := msg.Ack(ctx); err != nil {
				t.Fatalf("ack: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Receive did not return after enqueue")
		}
	})
}

func TestQueueReceiveRespectsContextCancel(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		q := mustNewQueue(t, bucket, "jobs/", blobster.WithReceiveBackoff(5*time.Millisecond, 10*time.Millisecond))

		t.Run("deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if _, err := q.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want DeadlineExceeded", err)
			}
		})
		t.Run("cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(30 * time.Millisecond)
				cancel()
			}()
			if _, err := q.Receive(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want Canceled", err)
			}
		})
	})
}

func TestQueueCompetingConsumersDrainAll(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		// Lease long and renew long so nothing is redelivered by lease expiry; any
		// duplicate delivery here would be the inherent at-least-once window of the
		// non-atomic two-delete ack, which the test tolerates. Every message must
		// be delivered at least once and the queue must fully drain.
		q := mustNewQueue(t, bucket, "jobs/",
			blobster.WithVisibilityLease(time.Minute),
			blobster.WithQueueRenewInterval(time.Hour),
			blobster.WithHeadWindow(512),
		)

		const messages = 60
		want := make(map[string]bool, messages)
		for i := range messages {
			want[enqueueString(t, q, fmt.Sprintf("body-%d", i), nil)] = true
		}

		const workers = 8
		var (
			wg       sync.WaitGroup
			deliv    atomic.Int64 // total deliveries, including any at-least-once duplicates
			mu       sync.Mutex
			distinct = make(map[string]bool, messages)
			once     sync.Once
		)
		done := make(chan struct{})
		finish := func() { once.Do(func() { close(done) }) }
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					msg, err := q.TryReceive(ctx)
					if errors.Is(err, blobster.ErrNoMessages) {
						select {
						case <-done:
							return
						case <-time.After(time.Millisecond):
						}
						continue
					}
					if err != nil {
						t.Errorf("TryReceive: %v", err)
						finish()
						return
					}
					deliv.Add(1)
					mu.Lock()
					distinct[msg.ID()] = true
					n := len(distinct)
					mu.Unlock()
					// Ack on the parent context so termination never aborts cleanup.
					if err := msg.Ack(ctx); err != nil {
						t.Errorf("Ack: %v", err)
					}
					if n == messages {
						finish()
						return
					}
				}
			}()
		}
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		if len(distinct) != messages {
			t.Fatalf("distinct delivered = %d, want %d (total deliveries %d)", len(distinct), messages, deliv.Load())
		}
		for id := range distinct {
			if !want[id] {
				t.Errorf("delivered unknown id %q", id)
			}
		}
		// Nothing should remain leased or stored once every message is acked.
		if _, err := q.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
			t.Fatalf("queue not drained: %v", err)
		}
	})
}

func TestQueueLargeStreamedPayload(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/")

		// A multi-megabyte payload exercises the streamed write/read path; the
		// renewer never touches the payload, only the tiny lease record.
		payload := bytes.Repeat([]byte("blobster-"), 700*1024) // ~6 MiB
		id, err := q.Enqueue(ctx, bytes.NewReader(payload), nil)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		msg, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("TryReceive: %v", err)
		}
		if msg.ID() != id {
			t.Fatalf("ID = %q, want %q", msg.ID(), id)
		}

		r, err := msg.Body(ctx)
		if err != nil {
			t.Fatalf("Body: %v", err)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
		}
		if err := msg.Ack(ctx); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}

func TestQueueAckNackIdempotent(t *testing.T) {
	t.Parallel()
	eachBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		q := mustNewQueue(t, bucket, "jobs/", blobster.WithQueueRenewInterval(time.Hour))
		enqueueString(t, q, "work", nil)

		msg, err := q.TryReceive(ctx)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if err := msg.Ack(ctx); err != nil {
			t.Fatalf("first ack: %v", err)
		}
		if err := msg.Ack(ctx); err != nil {
			t.Fatalf("second ack: %v", err)
		}
		// Nack after ack is a no-op (already settled) and must not resurrect the
		// message.
		if err := msg.Nack(ctx); err != nil {
			t.Fatalf("nack after ack: %v", err)
		}
		if _, err := q.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
			t.Fatalf("queue not empty after ack: %v", err)
		}
		select {
		case <-msg.Done():
		default:
			t.Fatal("Done not closed after ack")
		}
		if err := msg.Err(); err != nil {
			t.Fatalf("Err after clean ack = %v, want nil", err)
		}
	})
}

func TestQueueNoGoroutineLeak(t *testing.T) {
	// No t.Parallel: runtime.NumGoroutine is process-global. Go defers parallel
	// tests until the serial ones complete, so none run concurrently here — the
	// documented process-global exception to the t.Parallel rule.
	ctx := t.Context()
	q := mustNewQueue(t, mem.New(), "jobs/", blobster.WithQueueRenewInterval(5*time.Millisecond))
	enqueueString(t, q, "work", nil)

	base := runtime.NumGoroutine()
	msg, err := q.TryReceive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Ack waits for the renewer to exit, so the count should already be back; poll
	// briefly to absorb scheduler lag.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("renewer goroutine leaked: baseline %d, now %d", base, n)
	}
}

func TestQueueNewIDSortableAndFixedWidth(t *testing.T) {
	t.Parallel()
	clock := newManualClock()
	q := mustNewQueue(t, mem.New(), "jobs/", blobster.WithQueueClock(clock.Now))
	ctx := t.Context()

	var ids []string
	for range 50 {
		id, err := q.Enqueue(ctx, bytes.NewReader(nil), nil)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if len(id) != 26 {
			t.Fatalf("id %q has length %d, want 26", id, len(id))
		}
		ids = append(ids, id)
		clock.Advance(time.Millisecond)
	}

	// Ids minted at strictly increasing timestamps must sort in mint order, which
	// is what makes listing approximately FIFO.
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	for i := range ids {
		if ids[i] != sorted[i] {
			t.Fatalf("ids not lexically sorted by time at %d: %q vs %q", i, ids[i], sorted[i])
		}
	}
}
