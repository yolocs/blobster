//go:build cloud

package gcs_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/blobtest"
	"github.com/yolocs/blobster/gcs"
	"google.golang.org/api/iterator"
)

func TestCloudBucket(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	bucketName := os.Getenv("BLOBSTER_GCS_BUCKET")
	if bucketName == "" {
		t.Skip("set BLOBSTER_GCS_BUCKET to run cloud GCS integration tests")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("storage client Close: %v", err)
		}
	})

	basePrefix := os.Getenv("BLOBSTER_GCS_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-cloud-%d/", basePrefix, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupGCSObjects(context.Background(), t, client, bucketName, prefix)
	})

	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return gcs.New(client, bucketName, gcs.WithPrefix(prefix))
	})
}

// TestCloudCrossRegionCopy exercises the real CrossRegionCopier path: it writes
// a source object and copies it server-side into a destination via XCopyFrom,
// then verifies the bytes landed. By default source and destination are the same
// bucket (different prefixes), which still drives the rewrite and the async
// handle; set BLOBSTER_GCS_XCOPY_DEST_BUCKET to make it a genuine cross-bucket
// (and, with a differently-located bucket, cross-region) copy.
func TestCloudCrossRegionCopy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	bucketName := os.Getenv("BLOBSTER_GCS_BUCKET")
	if bucketName == "" {
		t.Skip("set BLOBSTER_GCS_BUCKET to run cloud GCS integration tests")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("storage client Close: %v", err)
		}
	})

	destBucket := os.Getenv("BLOBSTER_GCS_XCOPY_DEST_BUCKET")
	if destBucket == "" {
		destBucket = bucketName
	}

	basePrefix := os.Getenv("BLOBSTER_GCS_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-xcopy-%d/", basePrefix, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupGCSObjects(context.Background(), t, client, bucketName, prefix)
		if destBucket != bucketName {
			cleanupGCSObjects(context.Background(), t, client, destBucket, prefix)
		}
	})

	src := gcs.New(client, bucketName, gcs.WithPrefix(prefix+"src/"))
	dst := gcs.New(client, destBucket, gcs.WithPrefix(prefix+"dst/"))

	payload := []byte("cross-region copy payload")
	if err := src.WriteAll(ctx, "obj", payload, &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("WriteAll source: %v", err)
	}

	copier, ok := blobster.Bucket(dst).(blobster.CrossRegionCopier)
	if !ok {
		t.Fatal("gcs bucket does not implement CrossRegionCopier")
	}
	op, err := copier.XCopyFrom(ctx, "obj", src, "obj", nil)
	if err != nil {
		t.Fatalf("XCopyFrom: %v", err)
	}
	select {
	case <-op.Done():
	case <-time.After(2 * time.Minute):
		t.Fatal("cross-region copy did not complete")
	}
	if err := op.Err(); err != nil {
		t.Fatalf("copy operation: %v", err)
	}

	got, err := dst.ReadAll(ctx, "obj")
	if err != nil {
		t.Fatalf("ReadAll destination: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("copied payload = %q, want %q", string(got), string(payload))
	}
}

func cleanupGCSObjects(ctx context.Context, t *testing.T, client *storage.Client, bucketName, prefix string) {
	t.Helper()
	iter := client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			return
		}
		if err != nil {
			t.Errorf("cleanup list: %v", err)
			return
		}
		if attrs.Prefix != "" {
			continue
		}
		if err := client.Bucket(bucketName).Object(attrs.Name).Delete(ctx); err != nil {
			t.Errorf("cleanup delete %q: %v", attrs.Name, err)
		}
	}
}
