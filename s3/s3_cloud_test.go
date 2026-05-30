//go:build cloud

package s3_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/yolocs/blobster"
	"github.com/yolocs/blobster/blobtest"
	"github.com/yolocs/blobster/s3"
)

func TestCloudBucket(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	bucketName := os.Getenv("BLOBSTER_S3_BUCKET")
	if bucketName == "" {
		t.Skip("set BLOBSTER_S3_BUCKET to run cloud S3 integration tests")
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	client := awss3.NewFromConfig(cfg)

	basePrefix := os.Getenv("BLOBSTER_S3_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-cloud-%d/", basePrefix, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupS3Objects(context.Background(), t, client, bucketName, prefix)
	})

	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return s3.New(client, bucketName, s3.WithPrefix(prefix))
	})
}

func cleanupS3Objects(ctx context.Context, t *testing.T, client *awss3.Client, bucketName, prefix string) {
	t.Helper()
	paginator := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Errorf("cleanup list: %v", err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			}); err != nil {
				t.Errorf("cleanup delete %q: %v", aws.ToString(obj.Key), err)
			}
		}
	}
}
