package blobster_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/file"
	"github.com/yolocs/blobster/mem"
)

// bucketFactory builds a fresh real bucket to back a Locker.
type bucketFactory struct {
	name string
	make func(t *testing.T) blobster.Bucket
}

func lockBuckets() []bucketFactory {
	return []bucketFactory{
		{name: "mem", make: func(t *testing.T) blobster.Bucket { return mem.New() }},
		{name: "file", make: func(t *testing.T) blobster.Bucket { return file.New(t.TempDir()) }},
	}
}

func eachLockBucket(t *testing.T, fn func(t *testing.T, bucket blobster.Bucket)) {
	t.Helper()
	for _, f := range lockBuckets() {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			fn(t, f.make(t))
		})
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

// faultBucket wraps a real bucket and injects faults into WriteAll. It is fault
// injection at the backend seam (delegating real storage to the embedded
// bucket), not a storage mock, so it stays within the testing standards.
type faultBucket struct {
	blobster.Bucket
	mu         sync.Mutex
	onWriteAll func(key string) error
}

func (f *faultBucket) setWriteAllHook(fn func(key string) error) {
	f.mu.Lock()
	f.onWriteAll = fn
	f.mu.Unlock()
}

func (f *faultBucket) WriteAll(ctx context.Context, key string, p []byte, opts *blobster.WriterOptions, preconditions ...blobster.Precondition) error {
	f.mu.Lock()
	hook := f.onWriteAll
	f.mu.Unlock()
	if hook != nil {
		if err := hook(key); err != nil {
			return err
		}
	}
	return f.Bucket.WriteAll(ctx, key, p, opts, preconditions...)
}

func TestLockerAcquireReleaseReacquire(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

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
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

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
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

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
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newLockClock()
		// Renew interval far in the future so the holder never self-renews; we
		// drive expiry purely via the clock.
		l := blobster.NewLocker(bucket,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(time.Hour),
		)

		first, err := l.TryAcquire(ctx, "job", blobster.WithLockOwner("first"))
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}

		// Still within the lease: takeover must fail.
		if _, err := l.TryAcquire(ctx, "job", blobster.WithLockOwner("second")); !errors.Is(err, blobster.ErrLockHeld) {
			t.Fatalf("takeover before expiry: got %v, want ErrLockHeld", err)
		}

		clock.Advance(31 * time.Second)

		second, err := l.TryAcquire(ctx, "job", blobster.WithLockOwner("second"))
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
		if _, err := l.TryAcquire(ctx, "job", blobster.WithLockOwner("third")); !errors.Is(err, blobster.ErrLockHeld) {
			t.Fatalf("after displaced release, new holder should still hold: got %v", err)
		}

		if err := second.Release(ctx); err != nil {
			t.Fatalf("release second: %v", err)
		}
	})
}

func TestLockerMalformedRecordRecoverable(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

		// A record with no lease field (e.g. written out of band) must be
		// treated as expired so the lock can always be recovered.
		path := blobster.DefaultLockPrefix + "job"
		if err := bucket.WriteAll(ctx, path, nil, &blobster.WriterOptions{
			Metadata:                    map[string]string{"owner": "ghost"},
			DisableContentTypeDetection: true,
		}); err != nil {
			t.Fatalf("seed malformed record: %v", err)
		}

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire over malformed record: %v", err)
		}
		if err := held.Release(ctx); err != nil {
			t.Fatalf("release: %v", err)
		}
	})
}

func TestLockerDefaultOwnersUnique(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

		a, err := l.TryAcquire(ctx, "a")
		if err != nil {
			t.Fatalf("acquire a: %v", err)
		}
		defer a.Release(ctx)
		b, err := l.TryAcquire(ctx, "b")
		if err != nil {
			t.Fatalf("acquire b: %v", err)
		}
		defer b.Release(ctx)

		if a.Owner() == "" || b.Owner() == "" {
			t.Fatalf("empty default owner: a=%q b=%q", a.Owner(), b.Owner())
		}
		if a.Owner() == b.Owner() {
			t.Fatalf("default owners not unique: %q", a.Owner())
		}
	})
}

func TestLockerRenewKeepsLockHeld(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		// Freeze the clock so the lease can never lapse from wall-clock timing:
		// this isolates "the renewer runs and successfully CASes" from any
		// expiry race, which is what makes the test deterministic under -race,
		// parallel load, and file fsync. We prove renewal by observing the
		// record's version advance while the lock stays held.
		clock := newLockClock()
		l := blobster.NewLocker(bucket,
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
		attrs, err := bucket.Attributes(ctx, path)
		if err != nil {
			t.Fatalf("read initial version: %v", err)
		}
		v0 := attrs.Version

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

func TestLockerRenewSurvivesTransientError(t *testing.T) {
	t.Parallel()
	clock := newLockClock()
	fb := &faultBucket{Bucket: mem.New()}
	l := blobster.NewLocker(fb,
		blobster.WithLockClock(clock.Now),
		blobster.WithLeaseDuration(30*time.Second),
		blobster.WithRenewInterval(10*time.Millisecond),
	)
	ctx := t.Context()

	held, err := l.TryAcquire(ctx, "job")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer held.Release(ctx)

	// Fail exactly the first renew write, then recover. Frozen clock means the
	// lease cannot lapse meanwhile, so the lock must stay held and a later renew
	// must succeed (version advances).
	var calls atomic.Int64
	fb.setWriteAllHook(func(string) error {
		if calls.Add(1) == 1 {
			return errors.New("transient backend error")
		}
		return nil
	})

	path := blobster.DefaultLockPrefix + "job"
	attrs, err := fb.Attributes(ctx, path)
	if err != nil {
		t.Fatalf("initial version: %v", err)
	}
	v0 := attrs.Version

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-held.Done():
			t.Fatalf("lock lost on transient error: %v", held.Err())
		default:
		}
		if a, err := fb.Attributes(ctx, path); err == nil && a.Version != v0 {
			return // a renew succeeded after the injected failure
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("renewer did not recover from a transient error")
}

func TestLockerSurfacesBackendError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("backend exploded")
	fb := &faultBucket{Bucket: mem.New()}
	fb.setWriteAllHook(func(string) error { return sentinel })
	l := blobster.NewLocker(fb, blobster.WithRetryInterval(time.Millisecond))
	ctx := t.Context()

	if _, err := l.TryAcquire(ctx, "job"); !errors.Is(err, sentinel) {
		t.Fatalf("TryAcquire: got %v, want sentinel", err)
	}
	// Acquire must surface a non-ErrLockHeld error immediately, not loop forever.
	if _, err := l.Acquire(ctx, "job"); !errors.Is(err, sentinel) {
		t.Fatalf("Acquire: got %v, want sentinel", err)
	}
}

func TestLockerLostNotification(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		// Freeze the clock so the lease cannot lapse: the only way to lose the
		// lock here is the compare-and-swap failing because the record changed,
		// which is the path we want to assert (not an incidental expiry).
		clock := newLockClock()
		l := blobster.NewLocker(bucket,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(10*time.Millisecond),
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

func TestLockerReleaseAfterLoss(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newLockClock()
		l := blobster.NewLocker(bucket,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(10*time.Millisecond),
		)

		held, err := l.TryAcquire(ctx, "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		path := blobster.DefaultLockPrefix + "job"
		if err := bucket.WriteAll(ctx, path, []byte("x"), &blobster.WriterOptions{DisableContentTypeDetection: true}); err != nil {
			t.Fatalf("out-of-band overwrite: %v", err)
		}

		select {
		case <-held.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("never lost")
		}
		if !errors.Is(held.Err(), blobster.ErrLockLost) {
			t.Fatalf("Err() = %v, want ErrLockLost", held.Err())
		}
		// Release after a loss must be a safe no-op and must not delete the
		// record that displaced us (it carries a foreign value, not our owner).
		if err := held.Release(ctx); err != nil {
			t.Fatalf("release after loss: %v", err)
		}
	})
}

func TestLockerNoGoroutineLeak(t *testing.T) {
	// No t.Parallel: runtime.NumGoroutine is process-global. Go defers parallel
	// tests until the serial ones complete, so none run concurrently here. This
	// is the documented process-global exception to the t.Parallel rule.
	ctx := t.Context()
	l := blobster.NewLocker(mem.New(), blobster.WithRenewInterval(5*time.Millisecond))

	base := runtime.NumGoroutine()
	held, err := l.TryAcquire(ctx, "job")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Release waits for the renewer to exit, so the count should already be
	// back; poll briefly to absorb scheduler lag.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("renewer goroutine leaked: baseline %d, now %d", base, n)
	}
}

func TestLockerAcquireBlocksUntilReleased(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket,
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

func TestLockerAcquireWinsTakeover(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		clock := newLockClock()
		l := blobster.NewLocker(bucket,
			blobster.WithLockClock(clock.Now),
			blobster.WithLeaseDuration(30*time.Second),
			blobster.WithRenewInterval(time.Hour), // holder never self-renews
			blobster.WithRetryInterval(10*time.Millisecond),
		)

		first, err := l.TryAcquire(ctx, "job", blobster.WithLockOwner("first"))
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}

		acquired := make(chan *blobster.Lock, 1)
		go func() {
			lk, err := l.Acquire(ctx, "job", blobster.WithLockOwner("second"))
			if err != nil {
				t.Errorf("blocking acquire: %v", err)
				acquired <- nil
				return
			}
			acquired <- lk
		}()

		// Blocked while the lease is live.
		select {
		case <-acquired:
			t.Fatal("took over a live lock")
		case <-time.After(50 * time.Millisecond):
		}

		// Expire the first holder's lease; the blocked Acquire must take over.
		clock.Advance(31 * time.Second)

		select {
		case lk := <-acquired:
			if lk == nil {
				t.Fatal("takeover acquire failed")
			}
			if lk.Owner() != "second" {
				t.Errorf("owner = %q, want second", lk.Owner())
			}
			if err := lk.Release(ctx); err != nil {
				t.Fatalf("release winner: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Acquire did not take over after expiry")
		}

		// first is now a zombie; releasing it must not touch the successor.
		if err := first.Release(ctx); err != nil {
			t.Fatalf("zombie release: %v", err)
		}
	})
}

func TestLockerAcquireRespectsContextCancel(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		l := blobster.NewLocker(bucket,
			blobster.WithRenewInterval(time.Hour),
			blobster.WithRetryInterval(10*time.Millisecond),
		)

		held, err := l.TryAcquire(t.Context(), "job")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer held.Release(t.Context())

		t.Run("deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if _, err := l.Acquire(ctx, "job"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want DeadlineExceeded", err)
			}
		})
		t.Run("cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(30 * time.Millisecond)
				cancel()
			}()
			if _, err := l.Acquire(ctx, "job"); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want Canceled", err)
			}
		})
	})
}

func TestLockerWithLockPrefix(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket, blobster.WithLockPrefix("custom/locks"))

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
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket)

		for _, key := range []string{"", "/abs", "a/../b"} {
			if _, err := l.TryAcquire(ctx, key); !errors.Is(err, blobster.ErrInvalidLockKey) {
				t.Errorf("TryAcquire(%q): got %v, want ErrInvalidLockKey", key, err)
			}
		}
	})
}

func TestLockerReleaseIdempotent(t *testing.T) {
	t.Parallel()
	eachLockBucket(t, func(t *testing.T, bucket blobster.Bucket) {
		ctx := t.Context()
		l := blobster.NewLocker(bucket, blobster.WithRenewInterval(time.Hour))

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
