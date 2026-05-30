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
	"github.com/aws/aws-sdk-go-v2/credentials"
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

// TestCloudBucketR2 runs the shared conformance suite against Cloudflare R2
// through the same s3 driver — R2 is S3-compatible, so it needs no driver of its
// own (see docs/architecture.md). It is the empirical check for which
// capabilities R2 actually honors; in particular the "conditional writes and
// deletes are atomic" case verifies whether R2 enforces If-Match on DeleteObject,
// which its docs do not advertise.
func TestCloudBucketR2(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	bucketName := os.Getenv("BLOBSTER_R2_BUCKET")
	endpoint := os.Getenv("BLOBSTER_R2_ENDPOINT")
	accessKey := os.Getenv("BLOBSTER_R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("BLOBSTER_R2_SECRET_ACCESS_KEY")
	if bucketName == "" || endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("set BLOBSTER_R2_BUCKET, BLOBSTER_R2_ENDPOINT, BLOBSTER_R2_ACCESS_KEY_ID, and BLOBSTER_R2_SECRET_ACCESS_KEY to run cloud R2 integration tests")
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("auto"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		// aws-sdk-go-v2 computes a CRC32 request checksum by default, which R2
		// rejects as not implemented. Restore the prior when-required behavior.
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	basePrefix := os.Getenv("BLOBSTER_R2_PREFIX")
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
