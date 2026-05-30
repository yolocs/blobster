package gcs

import (
	"context"
	"errors"
	"fmt"
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

type fakeBackend struct {
	bucket                 blobster.Bucket
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
	b.lastReadKey = key
	return b.bucket.NewRangeReader(ctx, key, offset, length, opts)
}

func (b *fakeBackend) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error) {
	b.lastWriteKey = key
	b.lastWritePreconditions = preconditions
	return b.bucket.NewWriter(ctx, key, opts, preconditionsToList(preconditions)...)
}

func (b *fakeBackend) Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error {
	return b.bucket.Delete(ctx, key, preconditionsToList(preconditions)...)
}

func (b *fakeBackend) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions, preconditions blobster.Preconditions) error {
	return b.bucket.Copy(ctx, dstKey, srcKey, opts, preconditionsToList(preconditions)...)
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
