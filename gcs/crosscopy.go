package gcs

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/yolocs/blobster"
)

// rewriter performs a single server-side GCS rewrite of one object into another.
// It is the seam the cross-region copy path depends on, so prefix resolution and
// source-type gating are testable without a live GCS backend; the production
// implementation is clientRewriter.
type rewriter interface {
	rewrite(ctx context.Context, dstBucket, dstKey, srcBucket, srcKey string, opts *blobster.CopyOptions) error
}

// clientRewriter drives GCS's rewrite operation through a caller-owned client.
// The rewrite is issued against the destination object's client and merely names
// the source bucket, so — like the S3 cross-region copy — the destination
// client's credential must have read access to the source; the source Bucket's
// own client is intentionally not used. storage.Copier.Run drives the
// rewrite-token loop internally, so a single call handles cross-region,
// cross-bucket, and arbitrarily large objects without a manual multipart path.
type clientRewriter struct {
	client *storage.Client
}

func (r clientRewriter) rewrite(ctx context.Context, dstBucket, dstKey, srcBucket, srcKey string, opts *blobster.CopyOptions) error {
	dst := r.client.Bucket(dstBucket).Object(dstKey)
	src := r.client.Bucket(srcBucket).Object(srcKey)
	copier := dst.CopierFrom(src)
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(i any) bool { return blobster.AssignNative(copier, i) }); err != nil {
			return err
		}
	}
	_, err := copier.Run(ctx)
	return mapError(err)
}

// crossCopy carries the resolved physical identifiers for one cross-region copy.
// Bucket names and keys are already prefix-resolved.
type crossCopy struct {
	rewriter  rewriter
	dstBucket string
	dstKey    string
	srcBucket string
	srcKey    string
	opts      *blobster.CopyOptions
}

func (c crossCopy) run(ctx context.Context) error {
	return c.rewriter.rewrite(ctx, c.dstBucket, c.dstKey, c.srcBucket, c.srcKey, c.opts)
}
