package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/yolocs/blobster"
	"golang.org/x/sync/errgroup"
)

const (
	// maxSingleCopySize is S3's hard limit for a single CopyObject (5 GiB).
	// Larger objects must be copied with multipart UploadPartCopy.
	maxSingleCopySize = 5 * 1024 * 1024 * 1024
	// crossCopyPartSize is the default UploadPartCopy part size. It is scaled up
	// when an object would otherwise exceed maxCopyParts.
	crossCopyPartSize = 256 * 1024 * 1024
	// crossCopyConcurrency bounds the number of in-flight UploadPartCopy calls.
	crossCopyConcurrency = 8
	// maxCopyParts is S3's per-upload part ceiling.
	maxCopyParts = 10000
	// abortTimeout bounds the best-effort multipart cleanup so a detached abort
	// against an unresponsive endpoint cannot block the copy from completing.
	abortTimeout = 30 * time.Second
)

// copyAPI is the subset of *s3.Client the cross-region copy path uses. It exists
// so the multipart-copy orchestration (part ranges, bounded concurrency, abort
// on failure) is testable against a fake without a live S3 backend.
type copyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPartCopy(context.Context, *s3.UploadPartCopyInput, ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// crossCopy carries the resolved physical identifiers and tunables for one
// cross-region copy. Keys and bucket names are already prefix-resolved.
type crossCopy struct {
	api         copyAPI
	dstBucket   string
	dstKey      string
	copySource  string
	srcBucket   string
	srcKey      string
	opts        *blobster.CopyOptions
	maxSingle   int64
	partSize    int64
	maxParts    int
	concurrency int
}

// run heads the source to learn its size, then copies it server-side either as a
// single CopyObject (within the size limit) or as a multipart UploadPartCopy.
func (c crossCopy) run(ctx context.Context) error {
	head, err := c.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.srcBucket),
		Key:    aws.String(c.srcKey),
	})
	if err != nil {
		return mapError(err)
	}
	if aws.ToInt64(head.ContentLength) <= c.maxSingle {
		return c.single(ctx)
	}
	return c.multipart(ctx, head, aws.ToInt64(head.ContentLength))
}

// single performs an in-limit server-side CopyObject. BeforeCopy customizes the
// underlying CopyObjectInput here; it does not apply on the multipart path,
// which has no CopyObjectInput.
func (c crossCopy) single(ctx context.Context) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(c.dstBucket),
		Key:        aws.String(c.dstKey),
		CopySource: aws.String(c.copySource),
	}
	if c.opts != nil && c.opts.BeforeCopy != nil {
		if err := c.opts.BeforeCopy(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return err
		}
	}
	if _, err := c.api.CopyObject(ctx, input); err != nil {
		return mapError(err)
	}
	return nil
}

// multipart copies an over-limit object with concurrent ranged UploadPartCopy
// calls, preserving the source's content headers and user metadata on the
// destination. Any failure or cancellation aborts the upload so no parts are
// orphaned.
func (c crossCopy) multipart(ctx context.Context, head *s3.HeadObjectOutput, size int64) error {
	maxParts := int64(c.maxParts)
	partSize := c.partSize
	if (size+partSize-1)/partSize > maxParts {
		partSize = (size + maxParts - 1) / maxParts
	}
	partCount := int((size + partSize - 1) / partSize)

	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.dstBucket),
		Key:    aws.String(c.dstKey),
	}
	applyHeadMetadata(create, head)
	created, err := c.api.CreateMultipartUpload(ctx, create)
	if err != nil {
		return mapError(err)
	}
	uploadID := created.UploadId

	parts := make([]types.CompletedPart, partCount)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for i := 0; i < partCount; i++ {
		idx := i
		start := int64(i) * partSize
		end := start + partSize - 1
		if end >= size {
			end = size - 1
		}
		partNum := int32(i + 1)
		g.Go(func() error {
			out, err := c.api.UploadPartCopy(gctx, &s3.UploadPartCopyInput{
				Bucket:          aws.String(c.dstBucket),
				Key:             aws.String(c.dstKey),
				UploadId:        uploadID,
				CopySource:      aws.String(c.copySource),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				PartNumber:      aws.Int32(partNum),
			})
			if err != nil {
				return mapError(err)
			}
			parts[idx] = types.CompletedPart{
				ETag:       out.CopyPartResult.ETag,
				PartNumber: aws.Int32(partNum),
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		c.abort(uploadID)
		return err
	}

	if _, err := c.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(c.dstBucket),
		Key:             aws.String(c.dstKey),
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		c.abort(uploadID)
		return mapError(err)
	}
	return nil
}

// abort releases an incomplete multipart upload. It runs on a fresh bounded
// context, detached from the copy's (possibly already-cancelled) context, so a
// cancelled copy still frees its parts (which otherwise accrue storage cost until
// a lifecycle rule reaps them) without letting an unresponsive endpoint block the
// copy from reaching its terminal state. The cleanup itself is best-effort.
func (c crossCopy) abort(uploadID *string) {
	ctx, cancel := context.WithTimeout(context.Background(), abortTimeout)
	defer cancel()
	_, _ = c.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(c.dstBucket),
		Key:      aws.String(c.dstKey),
		UploadId: uploadID,
	})
}

// applyHeadMetadata mirrors a single CopyObject's default metadata-copy behavior
// onto the multipart path, which sets destination metadata at create time.
func applyHeadMetadata(create *s3.CreateMultipartUploadInput, head *s3.HeadObjectOutput) {
	if aws.ToString(head.CacheControl) != "" {
		create.CacheControl = head.CacheControl
	}
	if aws.ToString(head.ContentDisposition) != "" {
		create.ContentDisposition = head.ContentDisposition
	}
	if aws.ToString(head.ContentEncoding) != "" {
		create.ContentEncoding = head.ContentEncoding
	}
	if aws.ToString(head.ContentLanguage) != "" {
		create.ContentLanguage = head.ContentLanguage
	}
	if aws.ToString(head.ContentType) != "" {
		create.ContentType = head.ContentType
	}
	if len(head.Metadata) > 0 {
		create.Metadata = blobster.CloneMetadata(head.Metadata)
	}
}
