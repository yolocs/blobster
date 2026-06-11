package blobster_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/file"
	"github.com/yolocs/blobster/mem"
)

// This file reuses the shared test helpers defined in locker_test.go and
// queue_test.go. Watcher tests run against two real buckets standing in for
// two regions: the leader's (tailed read-only) and the follower's (the local
// queue, the lock, and the durable cursor).

const testWatchName = "leader-events"

func testWatchCursorPath() string { return ".blobster/watch/" + testWatchName + "/cursor" }

// newTestWatcher wires a leader queue's tail into a follower-local queue with
// fast polling. The local queue's long visibility lease and idle renewer mean
// no expiry-driven redelivery, so any duplicate delivery in these tests is a
// real replication duplicate.
func newTestWatcher(t *testing.T, leaderQ *blobster.Queue, follower blobster.Bucket, opts ...blobster.LockerOption) (*blobster.Watcher, *blobster.Queue) {
	t.Helper()
	localQ := mustNewQueue(t, follower, "events/",
		blobster.WithQueueVisibilityLease(time.Minute),
		blobster.WithQueueRenewInterval(time.Hour),
	)
	locker := blobster.NewLocker(follower, append([]blobster.LockerOption{blobster.WithRetryInterval(20 * time.Millisecond)}, opts...)...)
	w, err := blobster.NewWatcher(
		leaderQ.Tail(blobster.WithTailLag(0)),
		localQ,
		locker,
		blobster.WithWatchName(testWatchName),
		blobster.WithWatchPollBackoff(5*time.Millisecond, 20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	return w, localQ
}

// startWatcher runs w.Run on a goroutine and returns a stop function that
// cancels it and verifies the run ended cleanly (with the cancellation error).
func startWatcher(t *testing.T, w *blobster.Watcher) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("watcher Run returned %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("watcher did not stop after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// receiveDistinct drains q until n distinct messages are held (not acked),
// failing on any duplicate delivery — valid here because the test queues never
// expire leases on their own.
func receiveDistinct(t *testing.T, q *blobster.Queue, n int) map[string]*blobster.Message {
	t.Helper()
	ctx := t.Context()
	got := make(map[string]*blobster.Message, n)
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < n {
		if time.Now().After(deadline) {
			t.Fatalf("replicated %d of %d messages before timeout", len(got), n)
		}
		msg, err := q.TryReceive(ctx)
		if errors.Is(err, blobster.ErrNoMessages) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("TryReceive: %v", err)
		}
		if _, dup := got[msg.ID()]; dup {
			t.Fatalf("duplicate delivery of %q", msg.ID())
		}
		got[msg.ID()] = msg
	}
	return got
}

func ackAll(t *testing.T, msgs map[string]*blobster.Message) {
	t.Helper()
	for _, m := range msgs {
		if err := m.Ack(t.Context()); err != nil {
			t.Fatalf("Ack(%s): %v", m.ID(), err)
		}
	}
}

// maxID returns the lexically largest id — the value a fully caught-up cursor
// holds. (Enqueues within one millisecond are tie-broken by UUID, so the last
// id *minted* is not necessarily the largest.)
func maxID(ids ...string) string {
	max := ""
	for _, id := range ids {
		if id > max {
			max = id
		}
	}
	return max
}

// waitForCursor polls the durable cursor until it equals want, proving the
// watcher finished (and persisted) a tail pass up to that id.
func waitForCursor(t *testing.T, follower blobster.Bucket, want string) {
	t.Helper()
	ctx := t.Context()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		b, err := follower.ReadAll(ctx, testWatchCursorPath())
		if err == nil && string(b) == want {
			return
		}
		if err != nil && !errors.Is(err, blobster.ErrNotFound) {
			t.Fatalf("read cursor: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("cursor never reached %q", want)
}

// TestWatcherReplicatesEndToEnd: leader enqueues N, the watcher tails the
// leader's log read-only and re-enqueues locally, and follower workers receive
// and ack all N with bodies and attributes intact — across same- and
// cross-driver bucket pairs (the watcher is a plain cross-bucket read+write).
func TestWatcherReplicatesEndToEnd(t *testing.T) {
	t.Parallel()
	pairs := []struct {
		name string
		mk   func(t *testing.T) (leader, follower blobster.Bucket)
	}{
		{"mem-to-mem", func(t *testing.T) (blobster.Bucket, blobster.Bucket) { return mem.New(), mem.New() }},
		{"mem-to-file", func(t *testing.T) (blobster.Bucket, blobster.Bucket) { return mem.New(), file.New(t.TempDir()) }},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			leader, follower := p.mk(t)
			leaderQ := mustNewQueue(t, leader, "events/")
			w, localQ := newTestWatcher(t, leaderQ, follower)

			const n = 20
			want := make(map[string]string, n)
			for i := range n {
				body := fmt.Sprintf("evt-%d", i)
				id, err := leaderQ.Enqueue(ctx, strings.NewReader(body), &blobster.EnqueueOptions{
					Attributes: map[string]string{"seq": fmt.Sprint(i)},
				})
				if err != nil {
					t.Fatalf("leader enqueue: %v", err)
				}
				want[id] = body
			}

			stop := startWatcher(t, w)
			got := receiveDistinct(t, localQ, n)
			for id, msg := range got {
				body, ok := want[id]
				if !ok {
					t.Errorf("replicated unknown id %q", id)
					continue
				}
				gotBody, err := msg.ReadAll(ctx)
				if err != nil {
					t.Fatalf("read replicated body: %v", err)
				}
				if string(gotBody) != body {
					t.Errorf("body of %q = %q, want %q", id, gotBody, body)
				}
				if msg.Attributes()["seq"] == "" {
					t.Errorf("attributes of %q lost in replication", id)
				}
			}
			ackAll(t, got)
			if _, err := localQ.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
				t.Fatalf("local queue not drained: %v", err)
			}

			// The leader bucket was only ever read: tailing wrote no lease records.
			leases, _, err := leader.ListPage(ctx, blobster.FirstPageToken, 10, &blobster.ListOptions{Prefix: "events/lease/"})
			if err != nil {
				t.Fatalf("list leader leases: %v", err)
			}
			if len(leases) != 0 {
				t.Fatalf("watcher wrote %d lease records into the leader bucket", len(leases))
			}
			stop()
		})
	}
}

// TestWatcherResumesFromCursorAfterRestart: a stopped watcher restarts from its
// durable cursor — already-replicated (and acked) messages are not re-enqueued,
// and messages produced while it was down are picked up.
func TestWatcherResumesFromCursorAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	leader := mem.New()
	follower := mem.New()
	leaderQ := mustNewQueue(t, leader, "events/")
	w, localQ := newTestWatcher(t, leaderQ, follower)

	const n = 10
	first := make(map[string]bool, n)
	var firstIDs []string
	for i := range n {
		id, err := leaderQ.Enqueue(ctx, strings.NewReader(fmt.Sprintf("a-%d", i)), nil)
		if err != nil {
			t.Fatalf("leader enqueue: %v", err)
		}
		first[id] = true
		firstIDs = append(firstIDs, id)
	}

	stop := startWatcher(t, w)
	batch1 := receiveDistinct(t, localQ, n)
	// Ack before stopping: if the restarted watcher rewound, the re-created
	// acked ids would surface as extra deliveries below.
	ackAll(t, batch1)
	waitForCursor(t, follower, maxID(firstIDs...))
	stop()

	second := make(map[string]bool, n)
	for i := range n {
		id, err := leaderQ.Enqueue(ctx, strings.NewReader(fmt.Sprintf("b-%d", i)), nil)
		if err != nil {
			t.Fatalf("leader enqueue: %v", err)
		}
		second[id] = true
	}

	startWatcher(t, w)
	batch2 := receiveDistinct(t, localQ, n)
	for id := range batch2 {
		if !second[id] {
			t.Errorf("restarted watcher redelivered %q from before the cursor", id)
		}
	}
	ackAll(t, batch2)
	if _, err := localQ.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
		t.Fatalf("local queue not drained: %v", err)
	}
}

// TestWatcherIdempotentAcrossCursorRewind drives the crash window directly: the
// cursor is rewound to the beginning (a harder reset than any crash between
// "enqueue" and "persist cursor" can produce) and the watcher re-tails the
// whole log. The dedup-keyed re-enqueue must make every replay a no-op: no
// duplicate deliveries downstream.
func TestWatcherIdempotentAcrossCursorRewind(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	leader := mem.New()
	follower := mem.New()
	leaderQ := mustNewQueue(t, leader, "events/")
	w, localQ := newTestWatcher(t, leaderQ, follower)

	const n = 5
	var ids []string
	for i := range n {
		id, err := leaderQ.Enqueue(ctx, strings.NewReader(fmt.Sprintf("evt-%d", i)), nil)
		if err != nil {
			t.Fatalf("leader enqueue: %v", err)
		}
		ids = append(ids, id)
	}
	last := maxID(ids...)

	stop := startWatcher(t, w)
	got := receiveDistinct(t, localQ, n) // hold unacked: replays must dedup against them
	waitForCursor(t, follower, last)
	stop()

	// Rewind the cursor out of band and re-run: the watcher replays the log.
	if err := follower.WriteAll(ctx, testWatchCursorPath(), nil, &blobster.WriterOptions{DisableContentTypeDetection: true}); err != nil {
		t.Fatalf("rewind cursor: %v", err)
	}
	startWatcher(t, w)
	waitForCursor(t, follower, last)

	// The replay re-enqueued nothing: every message still exists locally, so
	// each EnqueueWithID was an existed=true no-op.
	if _, err := localQ.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
		t.Fatalf("replay produced a duplicate delivery: %v", err)
	}
	ackAll(t, got)
}

// TestWatcherTakeover: a standby watcher blocked on the lock takes over when
// the holder stops, resumes from the shared durable cursor, and replicates the
// messages the first holder never saw — no gap, no duplicate.
func TestWatcherTakeover(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	leader := mem.New()
	follower := mem.New()
	leaderQ := mustNewQueue(t, leader, "events/")
	wA, localQ := newTestWatcher(t, leaderQ, follower)
	wB, _ := newTestWatcher(t, leaderQ, follower)

	const n = 5
	all := make(map[string]bool, 2*n)
	var firstIDs []string
	for i := range n {
		id, err := leaderQ.Enqueue(ctx, strings.NewReader(fmt.Sprintf("a-%d", i)), nil)
		if err != nil {
			t.Fatalf("leader enqueue: %v", err)
		}
		all[id] = true
		firstIDs = append(firstIDs, id)
	}

	stopA := startWatcher(t, wA)
	gotFirst := receiveDistinct(t, localQ, n)
	waitForCursor(t, follower, maxID(firstIDs...))

	// B stands by on the lock while A holds it, then takes over on A's release.
	startWatcher(t, wB)
	stopA()

	for i := range n {
		id, err := leaderQ.Enqueue(ctx, strings.NewReader(fmt.Sprintf("b-%d", i)), nil)
		if err != nil {
			t.Fatalf("leader enqueue: %v", err)
		}
		all[id] = true
	}

	gotSecond := receiveDistinct(t, localQ, n)
	for id := range gotSecond {
		if !all[id] {
			t.Errorf("takeover replicated unknown id %q", id)
		}
		if _, dup := gotFirst[id]; dup {
			t.Errorf("takeover redelivered %q", id)
		}
	}
	ackAll(t, gotFirst)
	ackAll(t, gotSecond)
	if _, err := localQ.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
		t.Fatalf("local queue not drained: %v", err)
	}
}

// TestWatcherSkipsVanishedSourceMessages drives the listed-then-gone window
// deterministically: a message that is still listed by the tail but whose
// payload reads ErrNotFound (acked away or trimmed by the leader's retention
// after listing) is skipped, and the watcher advances past it without error.
func TestWatcherSkipsVanishedSourceMessages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	leader := newFaultBucket(mem.New())
	follower := mem.New()
	leaderQ := mustNewQueue(t, leader, "events/")
	w, localQ := newTestWatcher(t, leaderQ, follower)

	doomed, err := leaderQ.Enqueue(ctx, strings.NewReader("doomed"), nil)
	if err != nil {
		t.Fatalf("leader enqueue: %v", err)
	}
	survivor, err := leaderQ.Enqueue(ctx, strings.NewReader("survivor"), nil)
	if err != nil {
		t.Fatalf("leader enqueue: %v", err)
	}
	// The doomed payload stays listed, but its reads report it gone — exactly
	// what a concurrent ack/trim between listing and copying produces.
	leader.setAttributesHook(func(key string) error {
		if key == "msg/"+doomed {
			return blobster.ErrNotFound
		}
		return nil
	})

	startWatcher(t, w)
	got := receiveDistinct(t, localQ, 1)
	if _, ok := got[survivor]; !ok {
		t.Fatalf("survivor %q not replicated; got %v", survivor, got)
	}
	waitForCursor(t, follower, maxID(doomed, survivor)) // the cursor advanced past the vanished message
	ackAll(t, got)
	if _, err := localQ.TryReceive(ctx); !errors.Is(err, blobster.ErrNoMessages) {
		t.Fatalf("vanished message was replicated anyway: %v", err)
	}
}

func TestNewWatcherValidation(t *testing.T) {
	t.Parallel()
	bucket := mem.New()
	q := mustNewQueue(t, bucket, "events/")
	tail := q.Tail()
	locker := blobster.NewLocker(bucket)

	if _, err := blobster.NewWatcher(nil, q, locker); !errors.Is(err, blobster.ErrInvalidOption) {
		t.Errorf("nil source: got %v, want ErrInvalidOption", err)
	}
	if _, err := blobster.NewWatcher(tail, nil, locker); !errors.Is(err, blobster.ErrInvalidOption) {
		t.Errorf("nil local: got %v, want ErrInvalidOption", err)
	}
	if _, err := blobster.NewWatcher(tail, q, nil); !errors.Is(err, blobster.ErrInvalidOption) {
		t.Errorf("nil locker: got %v, want ErrInvalidOption", err)
	}
	for _, name := range []string{"", "a/b", "..", "."} {
		if _, err := blobster.NewWatcher(tail, q, locker, blobster.WithWatchName(name)); !errors.Is(err, blobster.ErrInvalidWatchName) {
			t.Errorf("WithWatchName(%q): got %v, want ErrInvalidWatchName", name, err)
		}
	}
	if _, err := blobster.NewWatcher(tail, q, locker, blobster.WithWatchName("ok-name")); err != nil {
		t.Errorf("valid construction: %v", err)
	}
}
