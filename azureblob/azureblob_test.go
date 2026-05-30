package azureblob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/google/go-cmp/cmp"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/blobtest"
	"github.com/yolocs/blobster/mem"
)

func TestBucketWithFakeBackendConforms(t *testing.T) {
	t.Parallel()
	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return newWithBackend(&fakeBackend{bucket: mem.New()}, "")
	})
}

func TestBucketAppliesPrefixAndStripsListResults(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := &fakeBackend{bucket: mem.New()}
	bucket := newWithBackend(fake, "root/")

	if err := bucket.WriteAll(ctx, "one.txt", []byte("one"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists); err != nil {
		t.Fatalf("WriteAll: %v", err)
	}
	if fake.lastWriteKey != "root/one.txt" {
		t.Fatalf("lastWriteKey = %q, want root/one.txt", fake.lastWriteKey)
	}
	if !fake.lastWritePreconditions.IfNotExists {
		t.Fatalf("IfNotExists was not passed to backend: %#v", fake.lastWritePreconditions)
	}

	if err := bucket.WriteAll(ctx, "nested/two.txt", []byte("two"), &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("WriteAll nested: %v", err)
	}

	got, err := bucket.ReadAll(ctx, "one.txt")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("ReadAll = %q, want one", string(got))
	}
	if fake.lastReadKey != "root/one.txt" {
		t.Fatalf("lastReadKey = %q, want root/one.txt", fake.lastReadKey)
	}

	page, _, err := bucket.ListPage(ctx, blobster.FirstPageToken, 10, &blobster.ListOptions{Prefix: ""})
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	var keys []string
	for _, obj := range page {
		keys = append(keys, obj.Key)
	}
	if diff := cmp.Diff([]string{"nested/two.txt", "one.txt"}, keys); diff != "" {
		t.Fatalf("listed keys mismatch (-want +got):\n%s", diff)
	}
}

func TestSignedURLUsesBackendCapability(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := &fakeBackend{bucket: mem.New(), signedURL: true}
	bucket := newWithBackend(fake, "root/")

	url, err := bucket.SignedURL(ctx, "object.txt", &blobster.SignedURLOptions{Method: "PUT", ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	if url != "https://signed.example/root/object.txt" {
		t.Fatalf("SignedURL = %q, want signed URL with physical key", url)
	}
	if !bucket.Capabilities().SignedURL {
		t.Fatal("Capabilities().SignedURL = false, want true")
	}
}

func TestAzureReaderSeekEndClampsBoundedRangeToObjectRemainder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	type openedRange struct {
		Offset int64
		Length int64
	}
	var opened []openedRange
	reader := &azureReader{
		ctx:    ctx,
		key:    "letters",
		start:  20,
		length: 20,
		open: func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, *blobster.Attributes, error) {
			opened = append(opened, openedRange{Offset: offset, Length: length})
			return io.NopCloser(bytes.NewReader(nil)), &blobster.Attributes{
				ContentType: "text/plain",
				Size:        26,
			}, nil
		},
	}
	if err := reader.reopen(0); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reader.Close()

	pos, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("SeekEnd: %v", err)
	}
	if pos != 6 {
		t.Fatalf("SeekEnd position = %d, want actual remaining range size 6", pos)
	}
	// Seeking to the end of a bounded range lands an empty range. Because the
	// object size is already known, the reader serves an empty body without
	// issuing a (zero-length, hence invalid) second download.
	wantOpened := []openedRange{
		{Offset: 20, Length: 20},
	}
	if diff := cmp.Diff(wantOpened, opened); diff != "" {
		t.Fatalf("opened ranges mismatch (-want +got):\n%s", diff)
	}

	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll after SeekEnd: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("ReadAll after SeekEnd returned %d bytes, want 0", len(rest))
	}
}

func TestAzureReaderClampsToRangeAndZeroLength(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// open serves the full object body from the requested offset regardless of the
	// requested length; the reader must clamp reads to the requested length itself.
	const body = "abcdefghijklmnopqrstuvwxyz"
	open := func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, *blobster.Attributes, error) {
		return io.NopCloser(strings.NewReader(body[offset:])), &blobster.Attributes{
			ContentType: "text/plain",
			Size:        int64(len(body)),
		}, nil
	}

	t.Run("bounded range clamps to length", func(t *testing.T) {
		t.Parallel()
		r := &azureReader{ctx: ctx, open: open, key: "k", start: 5, length: 3}
		if err := r.reopen(0); err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != "fgh" {
			t.Fatalf("ReadAll = %q, want fgh", string(got))
		}
	})

	t.Run("zero length range yields no bytes", func(t *testing.T) {
		t.Parallel()
		r := &azureReader{ctx: ctx, open: open, key: "k", start: 4, length: 0}
		if err := r.reopen(0); err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ReadAll on zero-length range = %q, want empty", string(got))
		}
	})
}

func TestAzureRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		offset, length int64
		want           blob.HTTPRange
	}{
		{name: "whole object", offset: 0, length: -1, want: blob.HTTPRange{}},
		{name: "open-ended from offset", offset: 5, length: -1, want: blob.HTTPRange{Offset: 5}},
		{name: "bounded", offset: 5, length: 10, want: blob.HTTPRange{Offset: 5, Count: 10}},
		{name: "zero length requests one byte", offset: 4, length: 0, want: blob.HTTPRange{Offset: 4, Count: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tc.want, azureRange(tc.offset, tc.length)); diff != "" {
				t.Fatalf("azureRange(%d, %d) mismatch (-want +got):\n%s", tc.offset, tc.length, diff)
			}
		})
	}
}

func TestTotalFromContentRange(t *testing.T) {
	t.Parallel()
	total, ok := totalFromContentRange(ptr("bytes 5-14/26"))
	if !ok || total != 26 {
		t.Fatalf("totalFromContentRange = (%d, %v), want (26, true)", total, ok)
	}
	if _, ok := totalFromContentRange(ptr("bytes 0-0/*")); ok {
		t.Fatal("totalFromContentRange with unknown total returned ok, want false")
	}
	if _, ok := totalFromContentRange(nil); ok {
		t.Fatal("totalFromContentRange(nil) returned ok, want false")
	}
}

type fakeBackend struct {
	bucket                 blobster.Bucket
	mu                     sync.Mutex
	signedURL              bool
	lastWriteKey           string
	lastWritePreconditions blobster.Preconditions
	lastReadKey            string
}

func (b *fakeBackend) As(i any) bool {
	return false
}

func (b *fakeBackend) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	return b.bucket.Attributes(ctx, key)
}

func (b *fakeBackend) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	b.mu.Lock()
	b.lastReadKey = key
	b.mu.Unlock()
	return b.bucket.NewRangeReader(ctx, key, offset, length, opts)
}

func (b *fakeBackend) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error) {
	b.mu.Lock()
	b.lastWriteKey = key
	b.lastWritePreconditions = preconditions
	b.mu.Unlock()
	return b.bucket.NewWriter(ctx, key, opts, preconditionsToList(preconditions)...)
}

func (b *fakeBackend) Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error {
	return b.bucket.Delete(ctx, key, preconditionsToList(preconditions)...)
}

func (b *fakeBackend) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	return b.bucket.Copy(ctx, dstKey, srcKey, opts)
}

func (b *fakeBackend) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	return b.bucket.ListPage(ctx, pageToken, pageSize, opts)
}

func (b *fakeBackend) IsAccessible(ctx context.Context) (bool, error) {
	return b.bucket.IsAccessible(ctx)
}

func (b *fakeBackend) SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error) {
	if !b.signedURL {
		return "", blobster.ErrUnsupported
	}
	return fmt.Sprintf("https://signed.example/%s", key), nil
}

func (b *fakeBackend) ErrorAs(err error, i any) bool {
	return errors.As(err, i)
}

func (b *fakeBackend) Capabilities() blobster.Capabilities {
	caps := b.bucket.Capabilities()
	caps.SignedURL = b.signedURL
	return caps
}

func preconditionsToList(preconditions blobster.Preconditions) []blobster.Precondition {
	var out []blobster.Precondition
	if preconditions.IfNotExists {
		out = append(out, blobster.IfNotExists)
	}
	if preconditions.IfMatch != "" {
		out = append(out, blobster.IfMatch(preconditions.IfMatch))
	}
	if preconditions.IfNotMatch != "" {
		out = append(out, blobster.IfNotMatch(preconditions.IfNotMatch))
	}
	return out
}
