package file_test

import (
	"errors"
	"testing"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/blobtest"
	"github.com/yolocs/blobster/file"
)

func TestBucket(t *testing.T) {
	t.Parallel()
	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return file.New(t.TempDir())
	})
}

func TestBucketRejectsKeysEscapingRoot(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := file.New(t.TempDir())

	if err := bucket.WriteAll(ctx, "../escape.txt", []byte("nope"), &blobster.WriterOptions{ContentType: "text/plain"}); err == nil {
		t.Fatal("WriteAll with escaping key succeeded, want error")
	}
	if _, err := bucket.ReadAll(ctx, "../escape.txt"); err == nil {
		t.Fatal("ReadAll with escaping key succeeded, want error")
	}
}

func TestSubBucketSharesConditionalState(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := file.New(t.TempDir())
	sub := bucket.Sub("nested/")

	if err := sub.WriteAll(ctx, "obj.txt", []byte("a"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists); err != nil {
		t.Fatalf("Sub WriteAll: %v", err)
	}
	// The same object is visible through the parent bucket at the composed key.
	got, err := bucket.ReadAll(ctx, "nested/obj.txt")
	if err != nil {
		t.Fatalf("parent ReadAll: %v", err)
	}
	if string(got) != "a" {
		t.Fatalf("parent ReadAll = %q, want a", string(got))
	}
	if err := sub.WriteAll(ctx, "obj.txt", []byte("b"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists); !errors.Is(err, blobster.ErrPreconditionFailed) {
		t.Fatalf("second create-only write error = %v, want ErrPreconditionFailed", err)
	}
}
