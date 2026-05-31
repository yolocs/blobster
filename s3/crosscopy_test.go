package s3

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/mem"
)

// fakeCopyAPI records calls and serves canned responses so the cross-region copy
// orchestration can be exercised without a live S3 backend.
type fakeCopyAPI struct {
	mu sync.Mutex

	headSize int64
	headErr  error
	head     *s3.HeadObjectOutput

	copyObjectInputs []*s3.CopyObjectInput
	createInputs     []*s3.CreateMultipartUploadInput
	partCopyInputs   []*s3.UploadPartCopyInput
	completeInputs   []*s3.CompleteMultipartUploadInput
	abortInputs      []*s3.AbortMultipartUploadInput

	partCopyHook func(*s3.UploadPartCopyInput) error
	completeHook func(*s3.CompleteMultipartUploadInput) error
	nextETag     int
}

func (f *fakeCopyAPI) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	if f.head != nil {
		return f.head, nil
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(f.headSize),
		ContentType:   aws.String("text/plain"),
		Metadata:      map[string]string{"k": "v"},
	}, nil
}

func (f *fakeCopyAPI) CopyObject(ctx context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyObjectInputs = append(f.copyObjectInputs, in)
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeCopyAPI) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createInputs = append(f.createInputs, in)
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil
}

func (f *fakeCopyAPI) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput, _ ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	if f.partCopyHook != nil {
		if err := f.partCopyHook(in); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partCopyInputs = append(f.partCopyInputs, in)
	f.nextETag++
	etag := fmt.Sprintf("etag-%d", f.nextETag)
	return &s3.UploadPartCopyOutput{CopyPartResult: &types.CopyPartResult{ETag: aws.String(etag)}}, nil
}

func (f *fakeCopyAPI) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if f.completeHook != nil {
		if err := f.completeHook(in); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeInputs = append(f.completeInputs, in)
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeCopyAPI) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abortInputs = append(f.abortInputs, in)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func newCrossCopy(api copyAPI, maxSingle, partSize int64) crossCopy {
	return crossCopy{
		api:         api,
		dstBucket:   "dst-bucket",
		dstKey:      "dst/key",
		copySource:  copySource("src-bucket", "src/key"),
		srcBucket:   "src-bucket",
		srcKey:      "src/key",
		maxSingle:   maxSingle,
		partSize:    partSize,
		maxParts:    maxCopyParts,
		concurrency: 4,
	}
}

func TestCrossCopySingleCopyObject(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 8}
	c := newCrossCopy(api, 16, 4)

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(api.copyObjectInputs) != 1 {
		t.Fatalf("CopyObject calls = %d, want 1", len(api.copyObjectInputs))
	}
	in := api.copyObjectInputs[0]
	if aws.ToString(in.Bucket) != "dst-bucket" || aws.ToString(in.Key) != "dst/key" {
		t.Fatalf("destination = %s/%s, want dst-bucket/dst/key", aws.ToString(in.Bucket), aws.ToString(in.Key))
	}
	if got, want := aws.ToString(in.CopySource), "src-bucket/src/key"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
	if len(api.createInputs) != 0 {
		t.Fatal("multipart upload created for an in-limit object")
	}
}

func TestCrossCopySingleAppliesBeforeCopy(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 8}
	c := newCrossCopy(api, 16, 4)
	c.opts = &blobster.CopyOptions{
		BeforeCopy: func(asFunc func(any) bool) error {
			var in *s3.CopyObjectInput
			if !asFunc(&in) {
				return errors.New("asFunc did not assign *s3.CopyObjectInput")
			}
			in.StorageClass = types.StorageClassGlacier
			return nil
		},
	}

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := api.copyObjectInputs[0].StorageClass; got != types.StorageClassGlacier {
		t.Fatalf("StorageClass = %q, want GLACIER", got)
	}
}

func TestCrossCopyMultipartRangesAndOrder(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 10}
	c := newCrossCopy(api, 4, 4) // size 10 > maxSingle 4 -> multipart, parts of 4 bytes

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(api.createInputs) != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", len(api.createInputs))
	}
	// Source metadata is mirrored onto the destination at create time.
	if got := aws.ToString(api.createInputs[0].ContentType); got != "text/plain" {
		t.Fatalf("create ContentType = %q, want text/plain", got)
	}

	var ranges []string
	for _, in := range api.partCopyInputs {
		ranges = append(ranges, aws.ToString(in.CopySourceRange))
	}
	sort.Strings(ranges)
	want := []string{"bytes=0-3", "bytes=4-7", "bytes=8-9"}
	if diff := cmp.Diff(want, ranges); diff != "" {
		t.Fatalf("part ranges mismatch (-want +got):\n%s", diff)
	}

	if len(api.completeInputs) != 1 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 1", len(api.completeInputs))
	}
	parts := api.completeInputs[0].MultipartUpload.Parts
	if len(parts) != 3 {
		t.Fatalf("completed parts = %d, want 3", len(parts))
	}
	for i, p := range parts {
		if got, want := aws.ToInt32(p.PartNumber), int32(i+1); got != want {
			t.Fatalf("part[%d] number = %d, want %d (parts must be ascending)", i, got, want)
		}
		if aws.ToString(p.ETag) == "" {
			t.Fatalf("part[%d] has no ETag", i)
		}
	}
	if len(api.abortInputs) != 0 {
		t.Fatal("abort called on a successful multipart copy")
	}
}

func TestCrossCopyMultipartAbortsOnPartError(t *testing.T) {
	t.Parallel()
	partErr := errors.New("part copy failed")
	api := &fakeCopyAPI{
		headSize: 12,
		partCopyHook: func(in *s3.UploadPartCopyInput) error {
			if aws.ToString(in.CopySourceRange) == "bytes=4-7" {
				return partErr
			}
			return nil
		},
	}
	c := newCrossCopy(api, 4, 4)

	err := c.run(t.Context())
	if !errors.Is(err, partErr) {
		t.Fatalf("run err = %v, want %v", err, partErr)
	}
	if len(api.abortInputs) != 1 {
		t.Fatalf("abort calls = %d, want 1", len(api.abortInputs))
	}
	if got := aws.ToString(api.abortInputs[0].UploadId); got != "upload-1" {
		t.Fatalf("aborted upload id = %q, want upload-1", got)
	}
	if len(api.completeInputs) != 0 {
		t.Fatal("CompleteMultipartUpload called despite a failed part")
	}
}

func TestCrossCopyHeadErrorPropagates(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headErr: errors.New("no such source")}
	c := newCrossCopy(api, 16, 4)
	if err := c.run(t.Context()); err == nil {
		t.Fatal("run err = nil, want head error")
	}
}

// TestCrossCopyMultipartAbortsOnCompleteError covers the second abort path: all
// parts succeed but CompleteMultipartUpload fails, so the upload must be aborted
// rather than left orphaned.
func TestCrossCopyMultipartAbortsOnCompleteError(t *testing.T) {
	t.Parallel()
	completeErr := errors.New("complete failed")
	api := &fakeCopyAPI{
		headSize:     12,
		completeHook: func(*s3.CompleteMultipartUploadInput) error { return completeErr },
	}
	c := newCrossCopy(api, 4, 4)

	err := c.run(t.Context())
	if !errors.Is(err, completeErr) {
		t.Fatalf("run err = %v, want %v", err, completeErr)
	}
	// All parts were copied before Complete was attempted.
	if len(api.partCopyInputs) != 3 {
		t.Fatalf("UploadPartCopy calls = %d, want 3", len(api.partCopyInputs))
	}
	if len(api.abortInputs) != 1 {
		t.Fatalf("abort calls = %d, want 1", len(api.abortInputs))
	}
	if got := aws.ToString(api.abortInputs[0].UploadId); got != "upload-1" {
		t.Fatalf("aborted upload id = %q, want upload-1", got)
	}
}

// TestCrossCopyMultipartCapsPartCount drives the partSize-recalculation branch:
// with the default part size the object would need more than maxParts parts, so
// the copy must grow the part size until the count fits, and the ranges must
// still tile the whole object with no gaps or overlap.
func TestCrossCopyMultipartCapsPartCount(t *testing.T) {
	t.Parallel()
	const size = 40
	api := &fakeCopyAPI{headSize: size}
	c := newCrossCopy(api, 4, 4) // partSize 4 -> 10 parts naively
	c.maxParts = 3               // force recalculation: 40 over 3 parts

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(api.partCopyInputs); got > c.maxParts {
		t.Fatalf("part count = %d, want <= maxParts %d", got, c.maxParts)
	}

	// The ranges must cover [0,size) exactly once, in order, with no hole.
	type span struct{ start, end int64 }
	spans := make([]span, 0, len(api.partCopyInputs))
	for _, in := range api.partCopyInputs {
		var s span
		if _, err := fmt.Sscanf(aws.ToString(in.CopySourceRange), "bytes=%d-%d", &s.start, &s.end); err != nil {
			t.Fatalf("parse range %q: %v", aws.ToString(in.CopySourceRange), err)
		}
		spans = append(spans, s)
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	var next int64
	for i, s := range spans {
		if s.start != next {
			t.Fatalf("span[%d] starts at %d, want %d (gap or overlap)", i, s.start, next)
		}
		if s.end < s.start {
			t.Fatalf("span[%d] is empty: %d-%d", i, s.start, s.end)
		}
		next = s.end + 1
	}
	if next != size {
		t.Fatalf("ranges cover up to %d, want %d", next, size)
	}
}

// TestCrossCopyMultipartBoundsConcurrency proves SetLimit actually caps the
// number of in-flight UploadPartCopy calls; a regression dropping the limit
// would otherwise pass every other test.
func TestCrossCopyMultipartBoundsConcurrency(t *testing.T) {
	t.Parallel()
	const limit = 2
	var inFlight, maxSeen atomic.Int64
	release := make(chan struct{})
	var once sync.Once

	api := &fakeCopyAPI{
		headSize: 24, // partSize 4 -> 6 parts, more than the limit
		partCopyHook: func(*s3.UploadPartCopyInput) error {
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			// Park until enough parts are confirmed concurrently in flight, so the
			// high-water mark reflects the real ceiling without time-based sleeps.
			if cur >= limit {
				once.Do(func() { close(release) })
			}
			<-release
			inFlight.Add(-1)
			return nil
		},
	}
	c := newCrossCopy(api, 4, 4)
	c.concurrency = limit

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := maxSeen.Load(); got > limit {
		t.Fatalf("max in-flight UploadPartCopy = %d, want <= %d", got, limit)
	}
}

// TestCrossCopyMultipartMirrorsHeadMetadata verifies every content header and
// the user metadata are copied from the source head onto the destination's
// CreateMultipartUpload, and that metadata is cloned (not aliased).
func TestCrossCopyMultipartMirrorsHeadMetadata(t *testing.T) {
	t.Parallel()
	meta := map[string]string{"team": "blobster"}
	api := &fakeCopyAPI{
		head: &s3.HeadObjectOutput{
			ContentLength:      aws.Int64(8),
			CacheControl:       aws.String("max-age=60"),
			ContentDisposition: aws.String("inline"),
			ContentEncoding:    aws.String("gzip"),
			ContentLanguage:    aws.String("en"),
			ContentType:        aws.String("application/json"),
			Metadata:           meta,
		},
	}
	c := newCrossCopy(api, 4, 4)

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(api.createInputs) != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", len(api.createInputs))
	}
	got := api.createInputs[0]
	fields := map[string]struct{ got, want string }{
		"CacheControl":       {aws.ToString(got.CacheControl), "max-age=60"},
		"ContentDisposition": {aws.ToString(got.ContentDisposition), "inline"},
		"ContentEncoding":    {aws.ToString(got.ContentEncoding), "gzip"},
		"ContentLanguage":    {aws.ToString(got.ContentLanguage), "en"},
		"ContentType":        {aws.ToString(got.ContentType), "application/json"},
	}
	for name, f := range fields {
		if f.got != f.want {
			t.Errorf("create %s = %q, want %q", name, f.got, f.want)
		}
	}
	if diff := cmp.Diff(map[string]string{"team": "blobster"}, got.Metadata); diff != "" {
		t.Fatalf("create Metadata mismatch (-want +got):\n%s", diff)
	}
	// Mutating the source head map must not alter the recorded create input.
	meta["team"] = "mutated"
	if got.Metadata["team"] != "blobster" {
		t.Fatal("destination metadata aliases the source head map")
	}
}

// TestCrossCopyMultipartSkipsEmptyHeadFields confirms the empty-string guards in
// applyHeadMetadata leave unset destination fields nil rather than sending empty
// headers.
func TestCrossCopyMultipartSkipsEmptyHeadFields(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{
		head: &s3.HeadObjectOutput{ContentLength: aws.Int64(8)},
	}
	c := newCrossCopy(api, 4, 4)

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := api.createInputs[0]
	if got.CacheControl != nil || got.ContentDisposition != nil || got.ContentEncoding != nil ||
		got.ContentLanguage != nil || got.ContentType != nil || got.Metadata != nil {
		t.Fatalf("empty head fields produced non-nil create fields: %#v", got)
	}
}

// TestCrossCopyMultipartIgnoresBeforeCopy documents and guards the asymmetry that
// BeforeCopy applies only to the single CopyObject path, never multipart.
func TestCrossCopyMultipartIgnoresBeforeCopy(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 8} // > maxSingle 4 -> multipart
	c := newCrossCopy(api, 4, 4)
	c.opts = &blobster.CopyOptions{
		BeforeCopy: func(func(any) bool) error {
			t.Error("BeforeCopy was invoked on the multipart path")
			return errors.New("should not run")
		},
	}

	if err := c.run(t.Context()); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestCrossCopyMultipartAbortSurvivesCancel proves the detached abort still fires
// (and completes) when the copy's own context is cancelled mid-flight — the
// background cleanup must not be cancelled along with the copy.
func TestCrossCopyMultipartAbortSurvivesCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	parked := make(chan struct{})
	var once sync.Once
	api := &fakeCopyAPI{
		headSize: 12,
		partCopyHook: func(*s3.UploadPartCopyInput) error {
			once.Do(func() { close(parked) })
			<-ctx.Done()
			return ctx.Err()
		},
	}
	c := newCrossCopy(api, 4, 4)

	errc := make(chan error, 1)
	go func() { errc <- c.run(ctx) }()

	<-parked
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
	// The abort ran on its own bounded context, not the cancelled one, so it was
	// still issued.
	if len(api.abortInputs) != 1 {
		t.Fatalf("abort calls = %d, want 1 (abort must survive copy cancellation)", len(api.abortInputs))
	}
}

// TestXCopyFromResolvesPrefixes verifies XCopyFrom glues the source and
// destination bucket prefixes into the physical CopySource and destination key,
// end to end through the *Bucket and backend, using the injectable copy API.
func TestXCopyFromResolvesPrefixes(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 4}
	dst := newWithBackend(&s3Backend{copyAPI: api, bucket: "dst-bucket"}, "dstroot/")
	src := newWithBackend(&s3Backend{copyAPI: api, bucket: "src-bucket"}, "srcroot/")

	op, err := dst.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	select {
	case <-op.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not complete")
	}
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(api.copyObjectInputs) != 1 {
		t.Fatalf("CopyObject calls = %d, want 1", len(api.copyObjectInputs))
	}
	in := api.copyObjectInputs[0]
	if got := aws.ToString(in.Key); got != "dstroot/out.bin" {
		t.Fatalf("destination key = %q, want dstroot/out.bin", got)
	}
	if got, want := aws.ToString(in.CopySource), "src-bucket/srcroot/in.bin"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
}

func TestXCopyFromRejectsNonS3Source(t *testing.T) {
	t.Parallel()
	dst := newWithBackend(&s3Backend{copyAPI: &fakeCopyAPI{}, bucket: "dst-bucket"}, "")
	_, err := dst.XCopyFrom(t.Context(), "out.bin", mem.New(), "in.bin", nil)
	if !errors.Is(err, blobster.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
}

// TestXCopyFromComposesSubPrefixes verifies a Sub() source and destination
// compose their prefixes into the resolved CopySource and destination key, so
// re-rooted buckets copy between subtrees correctly.
func TestXCopyFromComposesSubPrefixes(t *testing.T) {
	t.Parallel()
	api := &fakeCopyAPI{headSize: 4}
	dst := newWithBackend(&s3Backend{copyAPI: api, bucket: "dst-bucket"}, "dstroot/").Sub("nested/")
	src := newWithBackend(&s3Backend{copyAPI: api, bucket: "src-bucket"}, "srcroot/").Sub("deep/")

	copier, ok := dst.(blobster.CrossRegionCopier)
	if !ok {
		t.Fatal("Sub() destination does not implement CrossRegionCopier")
	}
	op, err := copier.XCopyFrom(t.Context(), "out.bin", src, "in.bin", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	<-op.Done()
	if err := op.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	in := api.copyObjectInputs[0]
	if got := aws.ToString(in.Key); got != "dstroot/nested/out.bin" {
		t.Fatalf("destination key = %q, want dstroot/nested/out.bin", got)
	}
	if got, want := aws.ToString(in.CopySource), "src-bucket/srcroot/deep/in.bin"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
}
