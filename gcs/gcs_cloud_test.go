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
