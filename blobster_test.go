package blobster_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/yolocs/blobster"
)

func TestCompilePreconditionsRejectsConflicts(t *testing.T) {
	t.Parallel()

	_, err := blobster.CompilePreconditions([]blobster.Precondition{
		blobster.IfNotExists,
		blobster.IfMatch("1"),
	})
	if !errors.Is(err, blobster.ErrInvalidOption) {
		t.Fatalf("CompilePreconditions error = %v, want ErrInvalidOption", err)
	}
}

func TestNormalizeMetadataLowercasesAndRejectsDuplicates(t *testing.T) {
	t.Parallel()

	got, err := blobster.NormalizeMetadata(map[string]string{"Owner": "tests"})
	if err != nil {
		t.Fatalf("NormalizeMetadata: %v", err)
	}
	if diff := cmp.Diff(map[string]string{"owner": "tests"}, got); diff != "" {
		t.Fatalf("NormalizeMetadata mismatch (-want +got):\n%s", diff)
	}

	if _, err := blobster.NormalizeMetadata(map[string]string{"Owner": "a", "owner": "b"}); !errors.Is(err, blobster.ErrInvalidOption) {
		t.Fatalf("NormalizeMetadata duplicate error = %v, want ErrInvalidOption", err)
	}
}

func TestListIteratorSkipsEmptyPages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	bucket := &iteratorBucket{
		pages: []iteratorPage{
			{next: []byte("next")},
			{objects: []*blobster.ListObject{{Key: "object"}}},
		},
	}
	iter := blobster.NewListIterator(bucket, nil)

	obj, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if obj.Key != "object" {
		t.Fatalf("Next key = %q, want object", obj.Key)
	}
	if _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after final page error = %v, want EOF", err)
	}
	if bucket.calls != 2 {
		t.Fatalf("ListPage calls = %d, want 2", bucket.calls)
	}
}

type iteratorBucket struct {
	blobster.Bucket
	pages []iteratorPage
	calls int
}

type iteratorPage struct {
	objects []*blobster.ListObject
	next    []byte
}

func (b *iteratorBucket) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	b.calls++
	if len(b.pages) == 0 {
		return nil, nil, io.EOF
	}
	page := b.pages[0]
	b.pages = b.pages[1:]
	return page.objects, page.next, nil
}
