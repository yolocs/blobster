package file_test

import (
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
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

// TestKeysCannotCollideWithMetadata guards against an object key colliding with
// another key's metadata sidecar. Keys ending in ".attrs" were the classic
// single-tree failure mode; here data and metadata live in separate subtrees so
// every key stays independently addressable.
func TestKeysCannotCollideWithMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := file.New(t.TempDir())
	opts := &blobster.WriterOptions{ContentType: "text/plain"}

	if err := bucket.WriteAll(ctx, "x", []byte("data-x"), opts); err != nil {
		t.Fatalf("WriteAll x: %v", err)
	}
	if err := bucket.WriteAll(ctx, "x.attrs", []byte("data-x-attrs"), opts); err != nil {
		t.Fatalf("WriteAll x.attrs: %v", err)
	}

	gotX, err := bucket.ReadAll(ctx, "x")
	if err != nil {
		t.Fatalf("ReadAll x: %v", err)
	}
	if string(gotX) != "data-x" {
		t.Fatalf("ReadAll x = %q, want data-x (clobbered by sidecar?)", string(gotX))
	}
	gotAttrs, err := bucket.ReadAll(ctx, "x.attrs")
	if err != nil {
		t.Fatalf("ReadAll x.attrs: %v", err)
	}
	if string(gotAttrs) != "data-x-attrs" {
		t.Fatalf("ReadAll x.attrs = %q, want data-x-attrs", string(gotAttrs))
	}
}

// TestCopyAssignsFreshIdentity verifies a copied object gets its own version
// token (so conditional ops on the copy don't alias the source's version),
// matching the mem/gcs/s3 drivers.
func TestCopyAssignsFreshIdentity(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := file.New(t.TempDir())
	opts := &blobster.WriterOptions{ContentType: "text/plain"}

	if err := bucket.WriteAll(ctx, "src", []byte("body"), opts); err != nil {
		t.Fatalf("WriteAll src: %v", err)
	}
	srcAttrs, err := bucket.Attributes(ctx, "src")
	if err != nil {
		t.Fatalf("Attributes src: %v", err)
	}
	if err := bucket.Copy(ctx, "dst", "src", nil); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	dstAttrs, err := bucket.Attributes(ctx, "dst")
	if err != nil {
		t.Fatalf("Attributes dst: %v", err)
	}
	if dstAttrs.Version == "" || dstAttrs.Version == srcAttrs.Version {
		t.Fatalf("copy version = %q, want a fresh token distinct from source %q", dstAttrs.Version, srcAttrs.Version)
	}
}

// TestListIncludesAllKeys guards against keys being hidden from listing by an
// internal naming convention (e.g. a temp-file prefix).
func TestListIncludesAllKeys(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := file.New(t.TempDir())
	opts := &blobster.WriterOptions{ContentType: "text/plain"}

	keys := []string{"plain", "w-looks-like-temp", "nested/obj"}
	for _, k := range keys {
		if err := bucket.WriteAll(ctx, k, []byte(k), opts); err != nil {
			t.Fatalf("WriteAll %q: %v", k, err)
		}
	}

	var listed []string
	it := bucket.List(&blobster.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		listed = append(listed, obj.Key)
	}
	slices.Sort(listed)
	want := []string{"nested/obj", "plain", "w-looks-like-temp"}
	if diff := cmp.Diff(want, listed); diff != "" {
		t.Fatalf("listed keys mismatch (-want +got):\n%s", diff)
	}
}
