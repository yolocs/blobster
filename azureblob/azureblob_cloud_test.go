//go:build cloud

package azureblob_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/azureblob"
	"github.com/yolocs/blobster/blobtest"
)

func TestCloudBucket(t *testing.T) {
	t.Parallel()

	connStr := os.Getenv("BLOBSTER_AZURE_CONNECTION_STRING")
	containerName := os.Getenv("BLOBSTER_AZURE_CONTAINER")
	if connStr == "" || containerName == "" {
		t.Skip("set BLOBSTER_AZURE_CONNECTION_STRING and BLOBSTER_AZURE_CONTAINER to run cloud Azure integration tests")
	}

	client, err := container.NewClientFromConnectionString(connStr, containerName, nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString: %v", err)
	}

	basePrefix := os.Getenv("BLOBSTER_AZURE_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-cloud-%d/", basePrefix, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupAzureBlobs(context.Background(), t, client, prefix)
	})

	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return azureblob.New(client, azureblob.WithPrefix(prefix))
	})
}

// TestCloudCrossRegionCopy exercises the real CrossRegionCopier path: it writes
// a source object and copies it server-side into a destination via XCopyFrom,
// then verifies the bytes landed. By default source and destination are the same
// container (different prefixes), a same-account copy that needs no SAS but still
// drives Copy Blob and the async handle. Set BLOBSTER_AZURE_XCOPY_DEST_CONNECTION_STRING
// and BLOBSTER_AZURE_XCOPY_DEST_CONTAINER (a different storage account) to make it
// a genuine cross-account copy, which exercises minting a read SAS from the
// source's own credential.
func TestCloudCrossRegionCopy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	connStr := os.Getenv("BLOBSTER_AZURE_CONNECTION_STRING")
	containerName := os.Getenv("BLOBSTER_AZURE_CONTAINER")
	if connStr == "" || containerName == "" {
		t.Skip("set BLOBSTER_AZURE_CONNECTION_STRING and BLOBSTER_AZURE_CONTAINER to run cloud Azure integration tests")
	}

	srcClient, err := container.NewClientFromConnectionString(connStr, containerName, nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString source: %v", err)
	}

	destConnStr := os.Getenv("BLOBSTER_AZURE_XCOPY_DEST_CONNECTION_STRING")
	destContainer := os.Getenv("BLOBSTER_AZURE_XCOPY_DEST_CONTAINER")
	destClient := srcClient
	cleanupDest := func() {}
	if destConnStr != "" && destContainer != "" {
		destClient, err = container.NewClientFromConnectionString(destConnStr, destContainer, nil)
		if err != nil {
			t.Fatalf("NewClientFromConnectionString dest: %v", err)
		}
	}

	basePrefix := os.Getenv("BLOBSTER_AZURE_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-xcopy-%d/", basePrefix, time.Now().UnixNano())
	if destClient != srcClient {
		cleanupDest = func() { cleanupAzureBlobs(context.Background(), t, destClient, prefix) }
	}
	t.Cleanup(func() {
		cleanupAzureBlobs(context.Background(), t, srcClient, prefix)
		cleanupDest()
	})

	src := azureblob.New(srcClient, azureblob.WithPrefix(prefix+"src/"))
	dst := azureblob.New(destClient, azureblob.WithPrefix(prefix+"dst/"))

	payload := []byte("cross-region copy payload")
	if err := src.WriteAll(ctx, "obj", payload, &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("WriteAll source: %v", err)
	}

	copier, ok := blobster.Bucket(dst).(blobster.CrossRegionCopier)
	if !ok {
		t.Fatal("azureblob bucket does not implement CrossRegionCopier")
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

func cleanupAzureBlobs(ctx context.Context, t *testing.T, client *container.Client, prefix string) {
	t.Helper()
	pager := client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Errorf("cleanup list: %v", err)
			return
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			if _, err := client.NewBlobClient(*item.Name).Delete(ctx, nil); err != nil {
				t.Errorf("cleanup delete %q: %v", *item.Name, err)
			}
		}
	}
}
