package azureblob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/mem"
)

// testAccountKey is the well-known Azurite development key: a valid base64 key
// so GetSASURL can sign a SAS offline (the signature is pure HMAC, no network).
const testAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func newSharedKeyBackend(t *testing.T, account, containerName string) *azureBackend {
	t.Helper()
	cred, err := container.NewSharedKeyCredential(account, testAccountKey)
	if err != nil {
		t.Fatalf("NewSharedKeyCredential: %v", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s", account, containerName)
	client, err := container.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		t.Fatalf("NewClientWithSharedKeyCredential: %v", err)
	}
	return &azureBackend{client: client}
}

type copyCall struct {
	dstKey string
	srcURL string
	opts   *blobster.CopyOptions
}

// fakeCopier records the destination key and source URL handed to the copy seam
// and optionally blocks until ctx is cancelled, so prefix/SAS resolution, error
// propagation, and cancellation are exercised without a live Azure backend.
type fakeCopier struct {
	mu    sync.Mutex
	calls []copyCall
	err   error
	block chan struct{}
}

func (f *fakeCopier) copyBlob(ctx context.Context, dstKey, srcURL string, opts *blobster.CopyOptions) error {
	f.mu.Lock()
	f.calls = append(f.calls, copyCall{dstKey, srcURL, opts})
	f.mu.Unlock()
	if f.block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.block:
		}
	}
	return f.err
}

func (f *fakeCopier) lastCall(t *testing.T) copyCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 {
		t.Fatalf("copyBlob calls = %d, want 1", len(f.calls))
	}
	return f.calls[0]
}

func waitDone(t *testing.T, op *blobster.CopyOperation) {
	t.Helper()
	select {
	case <-op.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not complete")
	}
}

// bucketWith pairs a destination backend (whose copier is faked) with a source
// backend, returning *Bucket wrappers at the given prefixes.
func bucketWith(dst *azureBackend, fc *fakeCopier, dstPrefix string, src *azureBackend, srcPrefix string) (*Bucket, *Bucket) {
	dst.copier = fc
	return newWithBackend(dst, dstPrefix), newWithBackend(src, srcPrefix)
}

// TestXCopyFromSameAccountUsesPlainURL verifies a same-account copy resolves
// both prefixes into the physical keys and hands the source's plain blob URL
// (no SAS) to the copy seam.
func TestXCopyFromSameAccountUsesPlainURL(t *testing.T) {
	t.Parallel()
	fc := &fakeCopier{}
	// Same account name => same host => same-account copy, regardless of container.
	dstBackend := newSharedKeyBackend(t, "acct", "dstcont")
	srcBackend := newSharedKeyBackend(t, "acct", "srccont")
	dst, src := bucketWith(dstBackend, fc, "dstroot/", srcBackend, "srcroot/")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	call := fc.lastCall(t)
	if call.dstKey != "dstroot/out.bin" {
		t.Fatalf("dstKey = %q, want dstroot/out.bin", call.dstKey)
	}
	// The blob SDK URL-encodes "/" within the blob name as %2F.
	if !strings.Contains(call.srcURL, "/srccont/srcroot%2Fin.bin") {
		t.Fatalf("srcURL = %q, want source blob path /srccont/srcroot%%2Fin.bin", call.srcURL)
	}
	if strings.Contains(call.srcURL, "sig=") {
		t.Fatalf("srcURL = %q, want no SAS for a same-account copy", call.srcURL)
	}
}

// TestXCopyFromCrossAccountMintsReadSAS verifies a cross-account copy appends a
// read SAS minted from the source's own credential to the source URL.
func TestXCopyFromCrossAccountMintsReadSAS(t *testing.T) {
	t.Parallel()
	fc := &fakeCopier{}
	dstBackend := newSharedKeyBackend(t, "dstacct", "dstcont")
	srcBackend := newSharedKeyBackend(t, "srcacct", "srccont")
	dst, src := bucketWith(dstBackend, fc, "", srcBackend, "")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}

	call := fc.lastCall(t)
	if !strings.HasPrefix(call.srcURL, "https://srcacct.blob.core.windows.net/srccont/in.bin?") {
		t.Fatalf("srcURL = %q, want source-account blob URL with query", call.srcURL)
	}
	if !strings.Contains(call.srcURL, "sig=") {
		t.Fatalf("srcURL = %q, want a SAS signature for a cross-account copy", call.srcURL)
	}
	if !strings.Contains(call.srcURL, "sp=r") {
		t.Fatalf("srcURL = %q, want read-only SAS permission (sp=r)", call.srcURL)
	}
}

// TestXCopyFromComposesSubPrefixes verifies Sub() prefixes on both sides compose
// into the resolved destination key and source URL path.
func TestXCopyFromComposesSubPrefixes(t *testing.T) {
	t.Parallel()
	fc := &fakeCopier{}
	dstBackend := newSharedKeyBackend(t, "acct", "dstcont")
	srcBackend := newSharedKeyBackend(t, "acct", "srccont")
	dstRoot, srcRoot := bucketWith(dstBackend, fc, "dstroot/", srcBackend, "srcroot/")
	dst := dstRoot.Sub("nested/")
	src := srcRoot.Sub("deep/")

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

	call := fc.lastCall(t)
	if call.dstKey != "dstroot/nested/out.bin" {
		t.Fatalf("dstKey = %q, want dstroot/nested/out.bin", call.dstKey)
	}
	if !strings.Contains(call.srcURL, "/srccont/srcroot%2Fdeep%2Fin.bin") {
		t.Fatalf("srcURL = %q, want source path /srccont/srcroot%%2Fdeep%%2Fin.bin", call.srcURL)
	}
}

func TestXCopyFromRejectsNonAzureSource(t *testing.T) {
	t.Parallel()
	dstBackend := newSharedKeyBackend(t, "acct", "dstcont")
	dst, _ := bucketWith(dstBackend, &fakeCopier{}, "", newSharedKeyBackend(t, "acct", "x"), "")
	if _, err := dst.XCopyFrom(t.Context(), "out.bin", mem.New(), "in.bin", nil); !errors.Is(err, blobster.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
}

// TestXCopyFromReportsCopyError verifies a copy failure surfaces through the
// handle's Err, not the XCopyFrom return.
func TestXCopyFromReportsCopyError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("copy boom")
	fc := &fakeCopier{err: sentinel}
	dstBackend := newSharedKeyBackend(t, "acct", "dstcont")
	dst, src := bucketWith(dstBackend, fc, "", newSharedKeyBackend(t, "acct", "srccont"), "")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	waitDone(t, op)
	if !errors.Is(op.Err(), sentinel) {
		t.Fatalf("Err = %v, want %v", op.Err(), sentinel)
	}
}

// TestXCopyFromCancels verifies cancelling the context aborts the in-flight copy
// and the handle reports the cancellation.
func TestXCopyFromCancels(t *testing.T) {
	t.Parallel()
	fc := &fakeCopier{block: make(chan struct{})}
	dstBackend := newSharedKeyBackend(t, "acct", "dstcont")
	dst, src := bucketWith(dstBackend, fc, "", newSharedKeyBackend(t, "acct", "srccont"), "")

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

// statusStep is one canned GetProperties result for the poll loop.
type statusStep struct {
	status blob.CopyStatusType
	err    error
}

type fakeStatusPoller struct {
	steps []statusStep
	calls int
}

func (f *fakeStatusPoller) GetProperties(ctx context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
	step := f.steps[f.calls]
	if f.calls < len(f.steps)-1 {
		f.calls++
	}
	if step.err != nil {
		return blob.GetPropertiesResponse{}, step.err
	}
	status := step.status
	return blob.GetPropertiesResponse{CopyStatus: &status}, nil
}

func TestPollCopyStatus(t *testing.T) {
	t.Parallel()

	t.Run("pending then success", func(t *testing.T) {
		t.Parallel()
		poller := &fakeStatusPoller{steps: []statusStep{
			{status: blob.CopyStatusTypePending},
			{status: blob.CopyStatusTypeSuccess},
		}}
		if err := pollCopyStatus(t.Context(), poller, "copy-1", time.Millisecond); err != nil {
			t.Fatalf("pollCopyStatus: %v", err)
		}
	})

	t.Run("tolerates transient errors then succeeds", func(t *testing.T) {
		t.Parallel()
		poller := &fakeStatusPoller{steps: []statusStep{
			{err: errors.New("transient 1")},
			{err: errors.New("transient 2")},
			{status: blob.CopyStatusTypeSuccess},
		}}
		if err := pollCopyStatus(t.Context(), poller, "copy-1", time.Millisecond); err != nil {
			t.Fatalf("pollCopyStatus: %v", err)
		}
	})

	t.Run("gives up after repeated errors", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("persistent boom")
		poller := &fakeStatusPoller{steps: []statusStep{{err: boom}}}
		if err := pollCopyStatus(t.Context(), poller, "copy-1", time.Millisecond); !errors.Is(err, boom) {
			t.Fatalf("pollCopyStatus err = %v, want %v", err, boom)
		}
	})

	t.Run("terminal non-success status fails", func(t *testing.T) {
		t.Parallel()
		poller := &fakeStatusPoller{steps: []statusStep{{status: blob.CopyStatusTypeFailed}}}
		err := pollCopyStatus(t.Context(), poller, "copy-1", time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "copy-1") {
			t.Fatalf("pollCopyStatus err = %v, want error naming copy-1", err)
		}
	})

	t.Run("cancellation while pending", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		poller := &fakeStatusPoller{steps: []statusStep{{status: blob.CopyStatusTypePending}}}
		if err := pollCopyStatus(ctx, poller, "copy-1", time.Hour); !errors.Is(err, context.Canceled) {
			t.Fatalf("pollCopyStatus err = %v, want context.Canceled", err)
		}
	})
}

// TestClientCopierAppliesBeforeCopy verifies the production copier hands the
// underlying *blob.StartCopyFromURLOptions to opts.BeforeCopy and short-circuits
// on its error, before any network call. It uses a no-credential client, which
// performs no I/O until StartCopyFromURL.
func TestClientCopierAppliesBeforeCopy(t *testing.T) {
	t.Parallel()
	client, err := container.NewClientWithNoCredential("https://acct.blob.core.windows.net/cont", nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	cc := clientCopier{client: client, interval: time.Millisecond}
	sentinel := errors.New("before-copy boom")
	var gotInput bool
	opts := &blobster.CopyOptions{
		BeforeCopy: func(as func(any) bool) error {
			var in *blob.StartCopyFromURLOptions
			gotInput = as(&in) && in != nil
			return sentinel
		},
	}
	err = cc.copyBlob(t.Context(), "out.bin", "https://src.example/in.bin", opts)
	if !errors.Is(err, sentinel) {
		t.Fatalf("copyBlob err = %v, want %v", err, sentinel)
	}
	if !gotInput {
		t.Fatal("BeforeCopy did not receive a *blob.StartCopyFromURLOptions")
	}
}
