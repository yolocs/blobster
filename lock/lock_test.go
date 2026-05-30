package lock_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/file"
	"github.com/yolocs/blobster/lock"
	"github.com/yolocs/blobster/mem"
)

// backendFactory builds a fresh backend plus the underlying bucket, so tests can
// also poke the record out of band to simulate takeovers.
type backendFactory struct {
	name string
	make func(t *testing.T) (lock.Backend, blobster.Bucket)
}

func backends() []backendFactory {
	return []backendFactory{
		{
			name: "mem",
			make: func(t *testing.T) (lock.Backend, blobster.Bucket) {
				b := mem.New()
				return lock.FromBucket(b), b
			},
		},
		{
			name: "file",
			make: func(t *testing.T) (lock.Backend, blobster.Bucket) {
				b := file.New(t.TempDir())
				return lock.FromBucket(b), b
			},
		},
	}
}

// fakeClock is a manually advanced clock for deterministic lease/expiry tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func eachBackend(t *testing.T, fn func(t *testing.T, f backendFactory)) {
	t.Helper()
	for _, f := range backends() {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			fn(t, f)
		})
	}
}

func TestAcquireReleaseReacquire(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}
		if held.Key() != "job" {
			t.Errorf("Key() = %q, want job", held.Key())
		}

		if _, err := l.TryAcquire(ctx, "job"); !errors.Is(err, lock.ErrLockHeld) {
			t.Fatalf("second acquire: got %v, want ErrLockHeld", err)
		}

		if err := held.Release(ctx); err != nil {
			t.Fatalf("release: %v", err)
		}

		again, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("reacquire after release: %v", err)
		}
		if err := again.Release(ctx); err != nil {
			t.Fatalf("release again: %v", err)
		}
	})
}

func TestDistinctKeysIndependent(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend)

		a, err := l.TryAcquire(ctx, "a")
		if err != nil {
			t.Fatalf("acquire a: %v", err)
		}
		b, err := l.TryAcquire(ctx, "b")
		if err != nil {
			t.Fatalf("acquire b: %v", err)
		}
		if err := a.Release(ctx); err != nil {
			t.Fatalf("release a: %v", err)
		}
		if err := b.Release(ctx); err != nil {
			t.Fatalf("release b: %v", err)
		}
	})
}

func TestConcurrentTryAcquireSingleWinner(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend)

		const contenders = 16
		var (
			wg      sync.WaitGroup
			winners atomic.Int64
			held    atomic.Int64
			start   = make(chan struct{})
		)
		var winner atomic.Pointer[lock.Lock]
		for range contenders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				lk, err := l.TryAcquire(ctx, "hot")
				switch {
				case err == nil:
					winners.Add(1)
					winner.Store(lk)
				case errors.Is(err, lock.ErrLockHeld):
					held.Add(1)
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
		if got := held.Load(); got != contenders-1 {
			t.Fatalf("held = %d, want %d", got, contenders-1)
		}
		if w := winner.Load(); w != nil {
			if err := w.Release(ctx); err != nil {
				t.Fatalf("release winner: %v", err)
			}
		}
	})
}

func TestTakeoverAfterExpiry(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		clock := newFakeClock()
		// Renew interval far in the future so the holder never self-renews; we
		// drive expiry purely via the clock.
		l := lock.New(backend,
			lock.WithClock(clock.Now),
			lock.WithLeaseDuration(30*time.Second),
			lock.WithRenewInterval(time.Hour),
		)

		first, err := l.TryAcquire(ctx, "job", lock.WithOwner("first"))
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}

		// Still within the lease: takeover must fail.
		if _, err := l.TryAcquire(ctx, "job", lock.WithOwner("second")); !errors.Is(err, lock.ErrLockHeld) {
			t.Fatalf("takeover before expiry: got %v, want ErrLockHeld", err)
		}

		clock.Advance(31 * time.Second)

		second, err := l.TryAcquire(ctx, "job", lock.WithOwner("second"))
		if err != nil {
			t.Fatalf("takeover after expiry: %v", err)
		}
		if second.Owner() != "second" {
			t.Errorf("owner = %q, want second", second.Owner())
		}

		// The displaced holder's Release must not disturb the new holder.
		if err := first.Release(ctx); err != nil {
			t.Fatalf("displaced release: %v", err)
		}
		if _, err := l.TryAcquire(ctx, "job", lock.WithOwner("third")); !errors.Is(err, lock.ErrLockHeld) {
			t.Fatalf("after displaced release, new holder should still hold: got %v", err)
		}

		if err := second.Release(ctx); err != nil {
			t.Fatalf("release second: %v", err)
		}
	})
}

func TestRenewKeepsLockHeld(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		// Real clock with a generous renew-to-lease margin (~12x): the background
		// renewer must keep the lock alive well past a single lease, with enough
		// headroom to stay reliable under -race, parallel load, and file fsync.
		l := lock.New(backend,
			lock.WithLeaseDuration(600*time.Millisecond),
			lock.WithRenewInterval(50*time.Millisecond),
		)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		// Span well beyond a single lease so a missing renewer would be caught.
		deadline := time.Now().Add(900 * time.Millisecond)
		for time.Now().Before(deadline) {
			if _, err := l.TryAcquire(ctx, "job"); !errors.Is(err, lock.ErrLockHeld) {
				t.Fatalf("lock not held during renew window: got %v", err)
			}
			select {
			case <-held.Done():
				t.Fatalf("lock unexpectedly lost: %v", held.Err())
			case <-time.After(40 * time.Millisecond):
			}
		}
	})
}

func TestLostNotification(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, bucket := f.make(t)
		l := lock.New(backend,
			lock.WithLeaseDuration(60*time.Millisecond),
			lock.WithRenewInterval(15*time.Millisecond),
		)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		// Forcibly overwrite the record out of band, bumping its version so the
		// holder's next renew CAS fails — a stand-in for a successful takeover.
		path := lock.DefaultPrefix + "job"
		if err := bucket.WriteAll(ctx, path, []byte("x"), &blobster.WriterOptions{DisableContentTypeDetection: true}); err != nil {
			t.Fatalf("out-of-band overwrite: %v", err)
		}

		select {
		case <-held.Done():
			if !errors.Is(held.Err(), lock.ErrLockLost) {
				t.Fatalf("Err() = %v, want ErrLockLost", held.Err())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("lost notification not delivered")
		}
	})
}

func TestAcquireBlocksUntilReleased(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend,
			lock.WithLeaseDuration(time.Second),
			lock.WithRenewInterval(time.Hour),
			lock.WithRetryInterval(10*time.Millisecond),
		)

		first, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		acquired := make(chan *lock.Lock, 1)
		go func() {
			lk, err := l.Acquire(ctx, "job")
			if err != nil {
				t.Errorf("blocking acquire: %v", err)
				acquired <- nil
				return
			}
			acquired <- lk
		}()

		select {
		case <-acquired:
			t.Fatal("Acquire returned while lock was held")
		case <-time.After(50 * time.Millisecond):
		}

		if err := first.Release(ctx); err != nil {
			t.Fatalf("release: %v", err)
		}

		select {
		case lk := <-acquired:
			if lk == nil {
				t.Fatal("blocking acquire failed")
			}
			if err := lk.Release(ctx); err != nil {
				t.Fatalf("release second: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Acquire did not return after release")
		}
	})
}

func TestAcquireRespectsContextCancel(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		backend, _ := f.make(t)
		l := lock.New(backend,
			lock.WithRenewInterval(time.Hour),
			lock.WithRetryInterval(10*time.Millisecond),
		)

		held, err := l.TryAcquire(t.Context(), "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(t.Context())

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if _, err := l.Acquire(ctx, "job"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Acquire under canceled ctx: got %v, want DeadlineExceeded", err)
		}
	})
}

func TestWithPrefix(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, bucket := f.make(t)
		l := lock.New(backend, lock.WithPrefix("custom/locks"))

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		if ok, err := bucket.Exists(ctx, "custom/locks/job"); err != nil || !ok {
			t.Fatalf("record at custom prefix: exists=%v err=%v", ok, err)
		}
		if ok, _ := bucket.Exists(ctx, lock.DefaultPrefix+"job"); ok {
			t.Fatal("record should not exist at default prefix")
		}
	})
}

func TestInvalidKeys(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend)

		for _, key := range []string{"", "/abs", "a/../b"} {
			if _, err := l.TryAcquire(ctx, key); !errors.Is(err, lock.ErrInvalidKey) {
				t.Errorf("TryAcquire(%q): got %v, want ErrInvalidKey", key, err)
			}
		}
	})
}

func TestReleaseIdempotent(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, f backendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := lock.New(backend, lock.WithRenewInterval(time.Hour))

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if err := held.Release(ctx); err != nil {
			t.Fatalf("first release: %v", err)
		}
		if err := held.Release(ctx); err != nil {
			t.Fatalf("second release: %v", err)
		}
		select {
		case <-held.Done():
		default:
			t.Fatal("Done not closed after release")
		}
		if err := held.Err(); err != nil {
			t.Fatalf("Err after clean release = %v, want nil", err)
		}
	})
}
