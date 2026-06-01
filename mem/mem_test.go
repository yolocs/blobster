package mem_test

import (
	"testing"

	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/blobtest"
	"github.com/yolocs/blobster/mem"
)

func TestBucket(t *testing.T) {
	t.Parallel()
	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return mem.New()
	})
}

func TestBucketMetadataAdvancesVersion(t *testing.T) {
	t.Parallel()
	blobtest.TestBucketMetadataAdvancesVersion(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return mem.New()
	})
}
