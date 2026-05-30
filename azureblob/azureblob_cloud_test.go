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
