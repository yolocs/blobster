package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

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

func TestGCSReaderSeekEndClampsBoundedRangeToObjectRemainder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	type openedRange struct {
		Offset int64
		Length int64
	}
	var opened []openedRange
	reader := &gcsReader{
		ctx:    ctx,
		key:    "letters",
		start:  20,
		length: 20,
		open: func(ctx context.Context, key string, offset, length int64) (objectReader, *blobster.Attributes, error) {
			opened = append(opened, openedRange{Offset: offset, Length: length})
			return &fakeObjectReader{Reader: bytes.NewReader(nil)}, &blobster.Attributes{
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
	wantOpened := []openedRange{
		{Offset: 20, Length: 20},
		{Offset: 26, Length: 0},
	}
	if diff := cmp.Diff(wantOpened, opened); diff != "" {
		t.Fatalf("opened ranges mismatch (-want +got):\n%s", diff)
	}
}

type fakeObjectReader struct {
	*bytes.Reader
}

func (r *fakeObjectReader) Close() error {
	return nil
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
