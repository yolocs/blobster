package gcs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/mem"
	"google.golang.org/api/option"
)

type rewriteCall struct {
	dstBucket string
	dstKey    string
	srcBucket string
	srcKey    string
	opts      *blobster.CopyOptions
}

// fakeRewriter records the physical identifiers handed to the rewrite seam and
// optionally blocks until ctx is cancelled, so prefix resolution, error
// propagation, and cancellation are exercised without a live GCS backend.
type fakeRewriter struct {
	mu    sync.Mutex
	calls []rewriteCall
	err   error
	block chan struct{}
}

func (r *fakeRewriter) rewrite(ctx context.Context, dstBucket, dstKey, srcBucket, srcKey string, opts *blobster.CopyOptions) error {
	r.mu.Lock()
	r.calls = append(r.calls, rewriteCall{dstBucket, dstKey, srcBucket, srcKey, opts})
	r.mu.Unlock()
	if r.block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.block:
		}
	}
	return r.err
}

func (r *fakeRewriter) lastCall(t *testing.T) rewriteCall {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) != 1 {
		t.Fatalf("rewrite calls = %d, want 1", len(r.calls))
	}
	return r.calls[0]
}

func waitDone(t *testing.T, op *blobster.CopyOperation) {
	t.Helper()
	select {
	case <-op.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not complete")
	}
}

// TestXCopyFromResolvesPrefixes verifies XCopyFrom glues the source and
// destination bucket prefixes into the physical keys handed to the rewrite,
// end to end through the *Bucket and backend, using the injectable rewriter.
func TestXCopyFromResolvesPrefixes(t *testing.T) {
	t.Parallel()
	rw := &fakeRewriter{}
	dst := newWithBackend(&storageBackend{rewriter: rw, bucket: "dst-bucket"}, "dstroot/")
	src := newWithBackend(&storageBackend{rewriter: rw, bucket: "src-bucket"}, "srcroot/")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	call := rw.lastCall(t)
	if call.dstBucket != "dst-bucket" || call.dstKey != "dstroot/out.bin" {
		t.Fatalf("destination = %q/%q, want dst-bucket/dstroot/out.bin", call.dstBucket, call.dstKey)
	}
	if call.srcBucket != "src-bucket" || call.srcKey != "srcroot/in.bin" {
		t.Fatalf("source = %q/%q, want src-bucket/srcroot/in.bin", call.srcBucket, call.srcKey)
	}
}

func TestXCopyFromRejectsNonGCSSource(t *testing.T) {
	t.Parallel()
	dst := newWithBackend(&storageBackend{rewriter: &fakeRewriter{}, bucket: "dst-bucket"}, "")
	_, err := dst.XCopyFrom(t.Context(), "out.bin", mem.New(), "in.bin", nil)
	if !errors.Is(err, blobster.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
}

// TestXCopyFromComposesSubPrefixes verifies a Sub() source and destination
// compose their prefixes into the resolved keys, so re-rooted buckets copy
// between subtrees correctly.
func TestXCopyFromComposesSubPrefixes(t *testing.T) {
	t.Parallel()
	rw := &fakeRewriter{}
	dst := newWithBackend(&storageBackend{rewriter: rw, bucket: "dst-bucket"}, "dstroot/").Sub("nested/")
	src := newWithBackend(&storageBackend{rewriter: rw, bucket: "src-bucket"}, "srcroot/").Sub("deep/")

	copier, ok := dst.(blobster.CrossRegionCopier)
	if !ok {
		t.Fatal("Sub() destination does not implement CrossRegionCopier")
	}
	op, err := copier.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	call := rw.lastCall(t)
	if call.dstKey != "dstroot/nested/out.bin" {
		t.Fatalf("destination key = %q, want dstroot/nested/out.bin", call.dstKey)
	}
	if call.srcKey != "srcroot/deep/in.bin" {
		t.Fatalf("source key = %q, want srcroot/deep/in.bin", call.srcKey)
	}
}

// TestXCopyFromReportsRewriteError verifies a rewrite failure surfaces through
// the handle's Err, not the XCopyFrom return.
func TestXCopyFromReportsRewriteError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("rewrite boom")
	rw := &fakeRewriter{err: sentinel}
	dst := newWithBackend(&storageBackend{rewriter: rw, bucket: "dst-bucket"}, "")
	src := newWithBackend(&storageBackend{rewriter: rw, bucket: "src-bucket"}, "")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if !errors.Is(op.Err(), sentinel) {
		t.Fatalf("Err = %v, want %v", op.Err(), sentinel)
	}
}

// TestXCopyFromCancels verifies cancelling the context aborts the in-flight
// rewrite and the handle reports the cancellation.
func TestXCopyFromCancels(t *testing.T) {
	t.Parallel()
	rw := &fakeRewriter{block: make(chan struct{})}
	dst := newWithBackend(&storageBackend{rewriter: rw, bucket: "dst-bucket"}, "")
	src := newWithBackend(&storageBackend{rewriter: rw, bucket: "src-bucket"}, "")

	ctx, cancel := context.WithCancel(t.Context())
	op, err := dst.XCopyFrom(ctx, "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	cancel()
	waitDone(t, op)
	if !errors.Is(op.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", op.Err())
	}
}

// TestClientRewriterAppliesBeforeCopy verifies the production rewriter hands the
// underlying *storage.Copier to opts.BeforeCopy and short-circuits on its error,
// before any network call. It uses an unauthenticated client, which performs no
// I/O during construction or copier setup.
func TestClientRewriterAppliesBeforeCopy(t *testing.T) {
	t.Parallel()
	client, err := storage.NewClient(t.Context(), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	rw := clientRewriter{client: client}
	sentinel := errors.New("before-copy boom")
	var gotCopier bool
	opts := &blobster.CopyOptions{
		BeforeCopy: func(as func(any) bool) error {
			var c *storage.Copier
			gotCopier = as(&c) && c != nil
			return sentinel
		},
	}
	err = rw.rewrite(t.Context(), "dst-bucket", "out.bin", "src-bucket", "in.bin", opts)
	if !errors.Is(err, sentinel) {
		t.Fatalf("rewrite err = %v, want %v", err, sentinel)
	}
	if !gotCopier {
		t.Fatal("BeforeCopy did not receive a *storage.Copier")
	}
}
