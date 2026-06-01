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

	// Note: the s3 driver deliberately does not run
	// blobtest.TestBucketMetadataAdvancesVersion. Its version token is the object
	// ETag — a content hash that a metadata-only self-copy leaves unchanged — so
	// UpdateMetadata returns the same token and cannot advance the version on a
	// metadata-only change (see docs/architecture.md).
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

	// Note: the s3 driver deliberately does not run
	// blobtest.TestBucketMetadataAdvancesVersion. Its version token is the object
	// ETag — a content hash that a metadata-only self-copy leaves unchanged — so
	// UpdateMetadata returns the same token and cannot advance the version on a
	// metadata-only change (see docs/architecture.md).
	blobtest.TestBucket(t, func(t *testing.T) blobster.Bucket {
		t.Helper()
		return s3.New(client, bucketName, s3.WithPrefix(prefix))
	})
}

// TestCloudCrossRegionCopy exercises the real CrossRegionCopier path: it writes
// a source object and copies it server-side into a destination via XCopyFrom,
// then verifies the bytes landed. By default source and destination are the same
// bucket (different prefixes), which still drives CopyObject and the async
// handle; set BLOBSTER_S3_XCOPY_DEST_BUCKET (and optionally
// BLOBSTER_S3_XCOPY_DEST_REGION) to make it a genuine cross-region copy. The
// multipart UploadPartCopy path is covered by unit tests, since a >5 GiB object
// is impractical here.
func TestCloudCrossRegionCopy(t *testing.T) {
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

	destBucket := os.Getenv("BLOBSTER_S3_XCOPY_DEST_BUCKET")
	destClient := client
	if destBucket == "" {
		destBucket = bucketName
	} else if region := os.Getenv("BLOBSTER_S3_XCOPY_DEST_REGION"); region != "" {
		destClient = awss3.NewFromConfig(cfg, func(o *awss3.Options) { o.Region = region })
	}

	basePrefix := os.Getenv("BLOBSTER_S3_PREFIX")
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	prefix := fmt.Sprintf("%sblobster-xcopy-%d/", basePrefix, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupS3Objects(context.Background(), t, client, bucketName, prefix)
		if destBucket != bucketName {
			cleanupS3Objects(context.Background(), t, destClient, destBucket, prefix)
		}
	})

	src := s3.New(client, bucketName, s3.WithPrefix(prefix+"src/"))
	dst := s3.New(destClient, destBucket, s3.WithPrefix(prefix+"dst/"))

	payload := []byte("cross-region copy payload")
	if err := src.WriteAll(ctx, "obj", payload, &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("WriteAll source: %v", err)
	}

	copier, ok := blobster.Bucket(dst).(blobster.CrossRegionCopier)
	if !ok {
		t.Fatal("s3 bucket does not implement CrossRegionCopier")
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
