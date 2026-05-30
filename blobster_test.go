package blobster_test

import (
	"errors"
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
