package blobster_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/file"
	"github.com/yolocs/blobster/mem"
)

// lockBackendFactory builds a fresh lock backend plus the underlying bucket, so
// tests can also poke the record out of band to simulate takeovers.
type lockBackendFactory struct {
	name string
	make func(t *testing.T) (blobster.LockBackend, blobster.Bucket)
}

func lockBackends() []lockBackendFactory {
	return []lockBackendFactory{
		{
			name: "mem",
			make: func(t *testing.T) (blobster.LockBackend, blobster.Bucket) {
				b := mem.New()
				return blobster.LockBackendFromBucket(b), b
			},
		},
		{
			name: "file",
			make: func(t *testing.T) (blobster.LockBackend, blobster.Bucket) {
				b := file.New(t.TempDir())
				return blobster.LockBackendFromBucket(b), b
			},
		},
	}
}

// lockClock is a manually advanced clock for deterministic lease/expiry tests.
type lockClock struct {
	mu sync.Mutex
	t  time.Time
}

func newLockClock() *lockClock {
	return &lockClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *lockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *lockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func eachLockBackend(t *testing.T, fn func(t *testing.T, f lockBackendFactory)) {
	t.Helper()
	for _, f := range lockBackends() {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			fn(t, f)
		})
	}
}

func TestLockerAcquireReleaseReacquire(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}
		if held.Key() != "job" {
			t.Errorf("Key() = %q, want job", held.Key())
		}

		if _, err := l.TryAcquire(ctx, "job"); !errors.Is(err, blobster.ErrLockHeld) {
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

func TestLockerDistinctKeysIndependent(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend)

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

func TestLockerConcurrentTryAcquireSingleWinner(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend)

		const contenders = 16
		var (
			wg      sync.WaitGroup
			winners atomic.Int64
			held    atomic.Int64
			start   = make(chan struct{})
		)
		var winner atomic.Pointer[blobster.Lock]
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
				case errors.Is(err, blobster.ErrLockHeld):
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

func TestLockerTakeoverAfterExpiry(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		clock := newLockClock()
		// Renew interval far in the future so the holder never self-renews; we
		// drive expiry purely via the clock.
		l := blobster.NewLocker(backend,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(time.Hour),
		)

		first, err := l.TryAcquire(ctx, "job", blobster.WithOwner("first"))
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}

		// Still within the lease: takeover must fail.
		if _, err := l.TryAcquire(ctx, "job", blobster.WithOwner("second")); !errors.Is(err, blobster.ErrLockHeld) {
			t.Fatalf("takeover before expiry: got %v, want ErrLockHeld", err)
		}

		clock.Advance(31 * time.Second)

		second, err := l.TryAcquire(ctx, "job", blobster.WithOwner("second"))
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
		if _, err := l.TryAcquire(ctx, "job", blobster.WithOwner("third")); !errors.Is(err, blobster.ErrLockHeld) {
			t.Fatalf("after displaced release, new holder should still hold: got %v", err)
		}

		if err := second.Release(ctx); err != nil {
			t.Fatalf("release second: %v", err)
		}
	})
}

func TestLockerRenewKeepsLockHeld(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		// Freeze the clock so the lease can never lapse from wall-clock timing:
		// this isolates "the renewer runs and successfully CASes" from any
		// expiry race, which is what makes the test deterministic under -race,
		// parallel load, and file fsync. We prove renewal by observing the
		// record's version advance while the lock stays held.
		clock := newLockClock()
		l := blobster.NewLocker(backend,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(20*time.Millisecond),
		)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		path := blobster.DefaultLockPrefix + "job"
		_, v0, err := backend.Get(ctx, path)
		if err != nil {
			t.Fatalf("read initial version: %v", err)
		}

		// Wait (generously) for at least one background renew to land, asserting
		// the lock stays held and unlost throughout.
		deadline := time.Now().Add(2 * time.Second)
		renewed := false
		for time.Now().Before(deadline) {
			select {
			case <-held.Done():
				t.Fatalf("lock unexpectedly lost: %v", held.Err())
			default:
			}
			if _, err := l.TryAcquire(ctx, "job"); !errors.Is(err, blobster.ErrLockHeld) {
				t.Fatalf("lock not held during renew window: got %v", err)
			}
			if _, v, err := backend.Get(ctx, path); err == nil && v != v0 {
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

func TestLockerLostNotification(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, bucket := f.make(t)
		l := blobster.NewLocker(backend,
			blobster.WithLeaseDuration(60*time.Millisecond),
			blobster.WithRenewInterval(15*time.Millisecond),
		)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		// Forcibly overwrite the record out of band, bumping its version so the
		// holder's next renew CAS fails — a stand-in for a successful takeover.
		path := blobster.DefaultLockPrefix + "job"
		if err := bucket.WriteAll(ctx, path, []byte("x"), &blobster.WriterOptions{DisableContentTypeDetection: true}); err != nil {
			t.Fatalf("out-of-band overwrite: %v", err)
		}

		select {
		case <-held.Done():
			if !errors.Is(held.Err(), blobster.ErrLockLost) {
				t.Fatalf("Err() = %v, want ErrLockLost", held.Err())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("lost notification not delivered")
		}
	})
}

func TestLockerAcquireBlocksUntilReleased(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend,
			blobster.WithLeaseDuration(time.Second),
			blobster.WithRenewInterval(time.Hour),
			blobster.WithRetryInterval(10*time.Millisecond),
		)

		first, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		acquired := make(chan *blobster.Lock, 1)
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

func TestLockerAcquireRespectsContextCancel(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend,
			blobster.WithRenewInterval(time.Hour),
			blobster.WithRetryInterval(10*time.Millisecond),
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

func TestLockerWithLockPrefix(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, bucket := f.make(t)
		l := blobster.NewLocker(backend, blobster.WithLockPrefix("custom/locks"))

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(ctx)

		if ok, err := bucket.Exists(ctx, "custom/locks/job"); err != nil || !ok {
			t.Fatalf("record at custom prefix: exists=%v err=%v", ok, err)
		}
		if ok, _ := bucket.Exists(ctx, blobster.DefaultLockPrefix+"job"); ok {
			t.Fatal("record should not exist at default prefix")
		}
	})
}

func TestLockerInvalidKeys(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend)

		for _, key := range []string{"", "/abs", "a/../b"} {
			if _, err := l.TryAcquire(ctx, key); !errors.Is(err, blobster.ErrInvalidLockKey) {
				t.Errorf("TryAcquire(%q): got %v, want ErrInvalidLockKey", key, err)
			}
		}
	})
}

func TestLockerReleaseIdempotent(t *testing.T) {
	t.Parallel()
	eachLockBackend(t, func(t *testing.T, f lockBackendFactory) {
		ctx := t.Context()
		backend, _ := f.make(t)
		l := blobster.NewLocker(backend, blobster.WithRenewInterval(time.Hour))

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
