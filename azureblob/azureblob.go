// Package azureblob implements a blobster.Bucket backed by Azure Blob Storage. It is
// built from a caller-owned *container.Client (blobster never reads ambient
// Azure configuration and never closes the client); a container is the bucket.
// Conditional writes use Azure's If-None-Match/If-Match ETag conditions; streamed
// writes go through the block-blob upload-stream API over an io.Pipe; server-side
// copy uses the asynchronous Copy Blob operation polled to completion.
package azureblob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/yolocs/blobster"
)

type Bucket struct {
	backend backend
	prefix  string
}

type Option func(*options)

type options struct {
	prefix       string
	copyInterval time.Duration
	sasExpiry    time.Duration
}

func WithPrefix(prefix string) Option {
	return func(opts *options) {
		opts.prefix = prefix
	}
}

// WithCopyPollInterval overrides how often the asynchronous Copy Blob status is
// polled to completion. The default is 200ms.
func WithCopyPollInterval(d time.Duration) Option {
	return func(opts *options) {
		opts.copyInterval = d
	}
}

// WithCrossAccountSASExpiry overrides how long the read SAS minted for a
// cross-account XCopyFrom source stays valid. The SAS must outlive the
// server-side copy; the default is one hour. It has no effect on same-account
// copies, which carry no SAS.
func WithCrossAccountSASExpiry(d time.Duration) Option {
	return func(opts *options) {
		opts.sasExpiry = d
	}
}

// New returns a Bucket backed by the container the caller-owned client points at.
// An Azure container is the unit that maps to a blobster bucket, so the container
// client carries both the account endpoint and the container name.
func New(client *container.Client, optFns ...Option) *Bucket {
	opts := options{copyInterval: defaultCopyPollInterval}
	for _, opt := range optFns {
		opt(&opts)
	}
	// A zero or negative interval would make the status poll loop spin without
	// backoff, so floor it at the default.
	if opts.copyInterval <= 0 {
		opts.copyInterval = defaultCopyPollInterval
	}
	return &Bucket{
		backend: &azureBackend{
			client:    client,
			copier:    clientCopier{client: client, interval: opts.copyInterval},
			sasExpiry: opts.sasExpiry,
		},
		prefix: opts.prefix,
	}
}

func newWithBackend(backend backend, prefix string) *Bucket {
	return &Bucket{backend: backend, prefix: prefix}
}

func (b *Bucket) As(i any) bool {
	return b.backend.As(i)
}

func (b *Bucket) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	return b.backend.Attributes(ctx, b.objectKey(key))
}

func (b *Bucket) Close() error {
	return nil
}

func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	return b.backend.Copy(ctx, b.objectKey(dstKey), b.objectKey(srcKey), opts)
}

// XCopyFrom implements blobster.CrossRegionCopier. The source must be an
// azureblob bucket (possibly in another region/account). The copy is issued
// against this (destination) bucket's client via Copy Blob, which authorizes the
// source read against the destination — so a cross-account source carries a
// short-lived read SAS minted from the source bucket's own client (see
// sourceURL). Same-account copies need no SAS. Same- vs cross-account is
// determined by comparing the source and destination client URL hosts, which is
// exact for standard Azure endpoints. opts.BeforeCopy customizes the underlying
// StartCopyFromURL.
func (b *Bucket) XCopyFrom(ctx context.Context, dstKey string, src blobster.Bucket, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error) {
	srcBucket, ok := src.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("%w: cross-region copy requires an azureblob source bucket", blobster.ErrUnsupported)
	}
	return b.backend.XCopyFrom(ctx, b.objectKey(dstKey), srcBucket.backend, srcBucket.objectKey(srcKey), opts)
}

func (b *Bucket) Delete(ctx context.Context, key string, preconditions ...blobster.Precondition) error {
	compiled, err := blobster.CompilePreconditions(preconditions)
	if err != nil {
		return err
	}
	return b.backend.Delete(ctx, b.objectKey(key), compiled)
}

func (b *Bucket) Download(ctx context.Context, key string, w io.Writer, opts *blobster.ReaderOptions) error {
	reader, err := b.NewReader(ctx, key, opts)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = io.Copy(w, reader)
	return err
}

func (b *Bucket) ErrorAs(err error, i any) bool {
	return b.backend.ErrorAs(err, i)
}

func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	if _, err := b.Attributes(ctx, key); err != nil {
		if errors.Is(err, blobster.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *Bucket) IsAccessible(ctx context.Context) (bool, error) {
	return b.backend.IsAccessible(ctx)
}

func (b *Bucket) List(opts *blobster.ListOptions) *blobster.ListIterator {
	return blobster.NewListIterator(b, opts)
}

func (b *Bucket) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	physicalOpts := &blobster.ListOptions{}
	if opts != nil {
		*physicalOpts = *opts
	}
	physicalOpts.Prefix = b.objectKey(physicalOpts.Prefix)
	page, next, err := b.backend.ListPage(ctx, pageToken, pageSize, physicalOpts)
	if err != nil {
		return nil, nil, err
	}
	for _, obj := range page {
		obj.Key = strings.TrimPrefix(obj.Key, b.prefix)
	}
	return page, next, nil
}

func (b *Bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	return b.backend.NewRangeReader(ctx, b.objectKey(key), offset, length, opts)
}

func (b *Bucket) NewReader(ctx context.Context, key string, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	return b.NewRangeReader(ctx, key, 0, -1, opts)
}

func (b *Bucket) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions ...blobster.Precondition) (blobster.Writer, error) {
	compiled, err := blobster.CompilePreconditions(preconditions)
	if err != nil {
		return nil, err
	}
	cloned, err := opts.Clone()
	if err != nil {
		return nil, err
	}
	return b.backend.NewWriter(ctx, b.objectKey(key), cloned, compiled)
}

func (b *Bucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	reader, err := b.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (b *Bucket) SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error) {
	return b.backend.SignedURL(ctx, b.objectKey(key), opts)
}

func (b *Bucket) Sub(prefix string) blobster.Bucket {
	return &Bucket{
		backend: b.backend,
		prefix:  b.prefix + prefix,
	}
}

func (b *Bucket) UpdateMetadata(ctx context.Context, key string, md map[string]string) (string, error) {
	normalized, err := blobster.NormalizeMetadata(md)
	if err != nil {
		return "", err
	}
	return b.backend.UpdateMetadata(ctx, b.objectKey(key), normalized)
}

func (b *Bucket) Upload(ctx context.Context, key string, r io.Reader, opts *blobster.WriterOptions, preconditions ...blobster.Precondition) error {
	cloned, err := blobster.RequireUploadOptions(opts)
	if err != nil {
		return err
	}
	writer, err := b.NewWriter(ctx, key, cloned, preconditions...)
	if err != nil {
		return err
	}
	if _, err := io.Copy(writer, r); err != nil {
		return errors.Join(err, writer.CloseWithError(err))
	}
	return writer.Close()
}

func (b *Bucket) WriteAll(ctx context.Context, key string, p []byte, opts *blobster.WriterOptions, preconditions ...blobster.Precondition) error {
	cloned, err := blobster.PrepareWriteAllOptions(opts, p)
	if err != nil {
		return err
	}
	writer, err := b.NewWriter(ctx, key, cloned, preconditions...)
	if err != nil {
		return err
	}
	if _, err := writer.Write(p); err != nil {
		return errors.Join(err, writer.CloseWithError(err))
	}
	return writer.Close()
}

func (b *Bucket) Capabilities() blobster.Capabilities {
	return b.backend.Capabilities()
}

func (b *Bucket) objectKey(key string) string {
	return b.prefix + key
}

type backend interface {
	As(i any) bool
	Attributes(ctx context.Context, key string) (*blobster.Attributes, error)
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error)
	NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error)
	UpdateMetadata(ctx context.Context, key string, md map[string]string) (string, error)
	Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error
	Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error
	XCopyFrom(ctx context.Context, dstKey string, src backend, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error)
	ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error)
	IsAccessible(ctx context.Context) (bool, error)
	SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error)
	ErrorAs(err error, i any) bool
	Capabilities() blobster.Capabilities
}

type azureBackend struct {
	client    *container.Client
	copier    copier
	sasExpiry time.Duration
}

func (b *azureBackend) As(i any) bool {
	return blobster.AssignNative(b.client, i)
}

func (b *azureBackend) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	resp, err := b.client.NewBlobClient(escapeKey(key, false)).GetProperties(ctx, nil)
	if err != nil {
		return nil, mapError(err)
	}
	return &blobster.Attributes{
		CacheControl:       deref(resp.CacheControl),
		ContentDisposition: deref(resp.ContentDisposition),
		ContentEncoding:    deref(resp.ContentEncoding),
		ContentLanguage:    deref(resp.ContentLanguage),
		ContentType:        deref(resp.ContentType),
		Metadata:           fromAzureMetadata(resp.Metadata),
		CreateTime:         derefTime(resp.CreationTime),
		ModTime:            derefTime(resp.LastModified),
		Size:               deref(resp.ContentLength),
		MD5:                bytes.Clone(resp.ContentMD5),
		Version:            etagVersion(resp.ETag),
		Native:             resp,
	}, nil
}

func (b *azureBackend) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	client := b.client.NewBlobClient(escapeKey(key, false))
	if opts != nil && opts.BeforeRead != nil {
		input := &blob.DownloadStreamOptions{}
		if err := opts.BeforeRead(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return nil, err
		}
	}
	r := &azureReader{
		ctx:    ctx,
		open:   newOpenReader(client),
		key:    key,
		start:  offset,
		length: length,
	}
	if err := r.reopen(0); err != nil {
		return nil, err
	}
	return r, nil
}

func newOpenReader(client *blob.Client) func(context.Context, string, int64, int64) (io.ReadCloser, *blobster.Attributes, error) {
	return func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, *blobster.Attributes, error) {
		resp, err := client.DownloadStream(ctx, &blob.DownloadStreamOptions{Range: azureRange(offset, length)})
		if err != nil {
			return nil, nil, mapError(err)
		}
		size := deref(resp.ContentLength)
		if total, ok := totalFromContentRange(resp.ContentRange); ok {
			size = total
		}
		attrs := &blobster.Attributes{
			CacheControl:    deref(resp.CacheControl),
			ContentEncoding: deref(resp.ContentEncoding),
			ContentType:     deref(resp.ContentType),
			Metadata:        fromAzureMetadata(resp.Metadata),
			ModTime:         derefTime(resp.LastModified),
			Size:            size,
			Version:         etagVersion(resp.ETag),
			Native:          resp,
		}
		return resp.Body, attrs, nil
	}
}

func (b *azureBackend) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error) {
	client := b.client.NewBlockBlobClient(escapeKey(key, false))
	headers := &blob.HTTPHeaders{}
	if opts.CacheControl != "" {
		headers.BlobCacheControl = &opts.CacheControl
	}
	if opts.ContentDisposition != "" {
		headers.BlobContentDisposition = &opts.ContentDisposition
	}
	if opts.ContentEncoding != "" {
		headers.BlobContentEncoding = &opts.ContentEncoding
	}
	if opts.ContentLanguage != "" {
		headers.BlobContentLanguage = &opts.ContentLanguage
	}
	if opts.ContentType != "" {
		headers.BlobContentType = &opts.ContentType
	}
	md, err := toAzureMetadata(opts.Metadata)
	if err != nil {
		return nil, err
	}
	w := &azureWriter{
		ctx:      ctx,
		client:   client,
		headers:  headers,
		metadata: md,
		conds:    modifiedConditions(preconditions),
		before:   opts.BeforeWrite,
	}
	// A caller-supplied MD5 is enforced atomically server-side by routing the
	// (necessarily bounded) body through a single Put Blob with a transactional
	// Content-MD5; UploadStream cannot validate a whole-object MD5 across blocks.
	// Unvalidated writes stay streaming and never buffer the whole blob.
	if len(opts.ContentMD5) > 0 {
		w.validateMD5 = bytes.Clone(opts.ContentMD5)
		return w, nil
	}
	if err := w.startStream(opts.BufferSize, opts.MaxConcurrency); err != nil {
		return nil, err
	}
	return w, nil
}

func (b *azureBackend) UpdateMetadata(ctx context.Context, key string, md map[string]string) (string, error) {
	// Set Blob Metadata replaces the whole metadata set and leaves the body
	// untouched; a nil/empty map clears it. The fresh ETag is the new version.
	azmd, err := toAzureMetadata(md)
	if err != nil {
		return "", err
	}
	resp, err := b.client.NewBlobClient(escapeKey(key, false)).SetMetadata(ctx, azmd, nil)
	if err != nil {
		return "", mapError(err)
	}
	return etagVersion(resp.ETag), nil
}

func (b *azureBackend) Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error {
	opts := &blob.DeleteOptions{AccessConditions: modifiedConditions(preconditions)}
	if _, err := b.client.NewBlobClient(escapeKey(key, false)).Delete(ctx, opts); err != nil {
		return mapError(err)
	}
	return nil
}

// Copy Blob is asynchronous, but the base Copy contract is synchronous, so the
// copier polls the destination's copy status to completion. The source is within
// this same container, so its plain URL needs no SAS.
func (b *azureBackend) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	srcURL := b.client.NewBlobClient(escapeKey(srcKey, false)).URL()
	return b.copier.copyBlob(ctx, dstKey, srcURL, opts)
}

func (b *azureBackend) XCopyFrom(ctx context.Context, dstKey string, src backend, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error) {
	srcBackend, ok := src.(*azureBackend)
	if !ok {
		return nil, fmt.Errorf("%w: cross-region copy requires an azureblob source bucket", blobster.ErrUnsupported)
	}
	c := crossCopy{
		copier:      b.copier,
		src:         srcBackend,
		dstKey:      dstKey,
		srcKey:      srcKey,
		sameAccount: b.accountHost() == srcBackend.accountHost(),
		opts:        opts,
	}
	return blobster.StartCopyOperation(ctx, c.run), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (b *azureBackend) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	if len(pageToken) == 0 {
		return nil, nil, io.EOF
	}
	if opts == nil {
		opts = &blobster.ListOptions{}
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	var marker *string
	if !bytes.Equal(pageToken, blobster.FirstPageToken) {
		marker = ptr(string(pageToken))
	}
	maxResults := int32(pageSize)
	escapedPrefix := escapeKey(opts.Prefix, true)

	if opts.Delimiter != "" {
		listOpts := &container.ListBlobsHierarchyOptions{Marker: marker, MaxResults: &maxResults}
		if escapedPrefix != "" {
			listOpts.Prefix = &escapedPrefix
		}
		if opts.BeforeList != nil {
			if err := opts.BeforeList(func(i any) bool { return blobster.AssignNative(listOpts, i) }); err != nil {
				return nil, nil, err
			}
		}
		pager := b.client.NewListBlobsHierarchyPager(escapeKey(opts.Delimiter, true), listOpts)
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, mapError(err)
		}
		seg := resp.Segment
		page := make([]*blobster.ListObject, 0, len(seg.BlobItems)+len(seg.BlobPrefixes))
		for _, prefix := range seg.BlobPrefixes {
			page = append(page, &blobster.ListObject{
				Key:    unescapeKey(deref(prefix.Name)),
				IsDir:  true,
				Native: prefix,
			})
		}
		for _, item := range seg.BlobItems {
			page = append(page, blobItemToObject(item))
		}
		return page, nextMarker(resp.NextMarker), nil
	}

	listOpts := &container.ListBlobsFlatOptions{Marker: marker, MaxResults: &maxResults}
	if escapedPrefix != "" {
		listOpts.Prefix = &escapedPrefix
	}
	if opts.BeforeList != nil {
		if err := opts.BeforeList(func(i any) bool { return blobster.AssignNative(listOpts, i) }); err != nil {
			return nil, nil, err
		}
	}
	pager := b.client.NewListBlobsFlatPager(listOpts)
	resp, err := pager.NextPage(ctx)
	if err != nil {
		return nil, nil, mapError(err)
	}
	page := make([]*blobster.ListObject, 0, len(resp.Segment.BlobItems))
	for _, item := range resp.Segment.BlobItems {
		page = append(page, blobItemToObject(item))
	}
	return page, nextMarker(resp.NextMarker), nil
}

func (b *azureBackend) IsAccessible(ctx context.Context) (bool, error) {
	_, err := b.client.GetProperties(ctx, nil)
	if err != nil {
		if errors.Is(mapError(err), blobster.ErrNotFound) {
			return false, nil
		}
		return false, mapError(err)
	}
	return true, nil
}

func (b *azureBackend) SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if opts == nil {
		opts = &blobster.SignedURLOptions{}
	}
	// GetSASURL signs only the blob path, permissions, and expiry; it cannot bind
	// a Content-Type to the request, so refuse rather than silently ignore the
	// caller's content-type enforcement.
	if opts.ContentType != "" || opts.EnforceAbsentContentType {
		return "", fmt.Errorf("%w: azureblob SAS URLs cannot enforce Content-Type", blobster.ErrUnsupported)
	}
	expiry := opts.Expiry
	if expiry == 0 {
		expiry = blobster.DefaultSignedURLExpiry
	}
	method := opts.Method
	if method == "" {
		method = http.MethodGet
	}
	perms, err := sasPermissions(method)
	if err != nil {
		return "", err
	}
	signed, err := b.client.NewBlobClient(escapeKey(key, false)).GetSASURL(perms, time.Now().Add(expiry), nil)
	if err != nil {
		return "", mapError(err)
	}
	return signed, nil
}

func (b *azureBackend) ErrorAs(err error, i any) bool {
	return errors.As(err, i)
}

func (b *azureBackend) Capabilities() blobster.Capabilities {
	return blobster.Capabilities{
		ConditionalWrites: true,
		Copy:              true,
		List:              true,
		ListPage:          true,
		RangeRead:         true,
		SignedURL:         true,
		CrossRegionCopy:   true,
	}
}

func sasPermissions(method string) (sas.BlobPermissions, error) {
	switch strings.ToUpper(method) {
	case http.MethodGet:
		return sas.BlobPermissions{Read: true}, nil
	case http.MethodPut:
		return sas.BlobPermissions{Read: true, Write: true, Create: true}, nil
	case http.MethodDelete:
		return sas.BlobPermissions{Delete: true}, nil
	default:
		return sas.BlobPermissions{}, fmt.Errorf("%w: unsupported signed URL method %q", blobster.ErrInvalidOption, method)
	}
}

func modifiedConditions(preconditions blobster.Preconditions) *blob.AccessConditions {
	switch {
	case preconditions.IfNotExists:
		any := azcore.ETagAny
		return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: &any}}
	case preconditions.IfMatch != "":
		etag := azcore.ETag(preconditions.IfMatch)
		return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag}}
	case preconditions.IfNotMatch != "":
		etag := azcore.ETag(preconditions.IfNotMatch)
		return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: &etag}}
	default:
		return nil
	}
}

func blobItemToObject(item *container.BlobItem) *blobster.ListObject {
	obj := &blobster.ListObject{
		Key:    unescapeKey(deref(item.Name)),
		Native: item,
	}
	if props := item.Properties; props != nil {
		obj.ModTime = derefTime(props.LastModified)
		obj.Size = deref(props.ContentLength)
		obj.MD5 = bytes.Clone(props.ContentMD5)
	}
	return obj
}

// azureReader implements Seek by closing the current DownloadStream body and
// reopening a new range request, mirroring the s3/gcs readers. Seek-heavy callers
// pay for a new request per seek.
type azureReader struct {
	ctx    context.Context
	open   func(context.Context, string, int64, int64) (io.ReadCloser, *blobster.Attributes, error)
	key    string
	start  int64
	length int64
	pos    int64
	reader io.ReadCloser
	attrs  *blobster.Attributes
	closed bool
}

func (r *azureReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("blobster azure: read after close")
	}
	// Clamp to the bytes remaining in the requested range. This bounds an empty
	// range (length 0, or a seek to the end) to zero bytes regardless of what the
	// service returns, and defends against a response exceeding the upper bound.
	remaining := r.rangeSize() - r.pos
	if remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.reader.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *azureReader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.pos + offset
	case io.SeekEnd:
		target = r.rangeSize() + offset
	default:
		return 0, fmt.Errorf("%w: invalid seek whence", blobster.ErrInvalidOption)
	}
	if target < 0 {
		return 0, fmt.Errorf("%w: negative seek offset", blobster.ErrInvalidOption)
	}
	if err := r.reopen(target); err != nil {
		return 0, err
	}
	return target, nil
}

func (r *azureReader) Close() error {
	r.closed = true
	if r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

func (r *azureReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, struct{ io.Reader }{r})
}

func (r *azureReader) As(i any) bool {
	if r.reader == nil {
		return false
	}
	return blobster.AssignNative(r.reader, i)
}

func (r *azureReader) ContentType() string {
	return r.attrs.ContentType
}

func (r *azureReader) ModTime() time.Time {
	return r.attrs.ModTime
}

func (r *azureReader) Size() int64 {
	return r.attrs.Size
}

func (r *azureReader) reopen(pos int64) error {
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return err
		}
	}
	size := r.length
	if r.attrs != nil {
		size = r.rangeSize()
	}
	length := int64(-1)
	if r.length >= 0 {
		if pos > size {
			pos = size
		}
		length = size - pos
	}
	// Azure has no zero-length range request. When we already know the object's
	// size, a seek that lands an empty bounded range serves an empty body directly
	// rather than issuing an invalid request.
	if length == 0 && r.attrs != nil {
		r.reader = io.NopCloser(bytes.NewReader(nil))
		r.pos = pos
		r.closed = false
		return nil
	}
	reader, attrs, err := r.open(r.ctx, r.key, r.start+pos, length)
	if err != nil {
		return err
	}
	r.reader = reader
	r.attrs = attrs
	r.pos = pos
	r.closed = false
	return nil
}

func (r *azureReader) rangeSize() int64 {
	if r.attrs == nil {
		return r.length
	}
	size := r.attrs.Size - r.start
	if size < 0 {
		size = 0
	}
	if r.length >= 0 && r.length < size {
		return r.length
	}
	return size
}

type azureWriter struct {
	ctx      context.Context
	client   *blockblob.Client
	headers  *blob.HTTPHeaders
	metadata map[string]*string
	conds    *blob.AccessConditions
	before   func(asFunc func(any) bool) error

	// Streaming path (no MD5 validation).
	pw   *io.PipeWriter
	done chan struct{}
	err  error

	// Buffered path (MD5 validation): the whole body is held until Close so a
	// single Put Blob can validate it transactionally.
	validateMD5 []byte
	buf         bytes.Buffer

	closed bool
}

func (w *azureWriter) startStream(bufferSize, concurrency int) error {
	streamOpts := &blockblob.UploadStreamOptions{
		HTTPHeaders:      w.headers,
		Metadata:         w.metadata,
		AccessConditions: w.conds,
	}
	if bufferSize > 0 {
		streamOpts.BlockSize = int64(bufferSize)
	}
	if concurrency > 0 {
		streamOpts.Concurrency = concurrency
	}
	if w.before != nil {
		if err := w.before(func(i any) bool { return blobster.AssignNative(streamOpts, i) }); err != nil {
			return err
		}
	}
	pr, pw := io.Pipe()
	w.pw = pw
	w.done = make(chan struct{})
	go func() {
		_, err := w.client.UploadStream(w.ctx, pr, streamOpts)
		w.err = mapError(err)
		// Unblock any in-flight Write once the upload has stopped consuming.
		pr.CloseWithError(err)
		close(w.done)
	}()
	return nil
}

func (w *azureWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("blobster azure: write after close")
	}
	if w.validateMD5 != nil {
		return w.buf.Write(p)
	}
	return w.pw.Write(p)
}

func (w *azureWriter) ReadFrom(r io.Reader) (int64, error) {
	if w.closed {
		return 0, errors.New("blobster azure: read from after close")
	}
	if w.validateMD5 != nil {
		return w.buf.ReadFrom(r)
	}
	return io.Copy(w.pw, r)
}

func (w *azureWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.validateMD5 != nil {
		return w.commitBuffered()
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.done
	return w.err
}

func (w *azureWriter) CloseWithError(cause error) error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.validateMD5 != nil {
		// Nothing was uploaded yet; dropping the buffer aborts the write.
		return nil
	}
	if cause == nil {
		cause = errors.New("blobster azure: write aborted")
	}
	w.pw.CloseWithError(cause)
	<-w.done
	return nil
}

func (w *azureWriter) commitBuffered() error {
	uploadOpts := &blockblob.UploadOptions{
		HTTPHeaders:             w.headers,
		Metadata:                w.metadata,
		AccessConditions:        w.conds,
		TransactionalValidation: blob.TransferValidationTypeMD5(w.validateMD5),
	}
	if w.before != nil {
		if err := w.before(func(i any) bool { return blobster.AssignNative(uploadOpts, i) }); err != nil {
			return err
		}
	}
	body := streaming.NopCloser(bytes.NewReader(w.buf.Bytes()))
	if _, err := w.client.Upload(w.ctx, body, uploadOpts); err != nil {
		return mapError(err)
	}
	return nil
}

func azureRange(offset, length int64) blob.HTTPRange {
	if offset <= 0 && length < 0 {
		return blob.HTTPRange{}
	}
	if length < 0 {
		return blob.HTTPRange{Offset: offset}
	}
	if length == 0 {
		// Azure has no zero-length range; request a single byte then discard. The
		// caller reads nothing because rangeSize bounds Read at zero.
		return blob.HTTPRange{Offset: offset, Count: 1}
	}
	return blob.HTTPRange{Offset: offset, Count: length}
}

func totalFromContentRange(contentRange *string) (int64, bool) {
	if contentRange == nil {
		return 0, false
	}
	idx := strings.LastIndex(*contentRange, "/")
	if idx < 0 {
		return 0, false
	}
	total := (*contentRange)[idx+1:]
	if total == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func etagVersion(etag *azcore.ETag) string {
	if etag == nil {
		return ""
	}
	return string(*etag)
}

func nextMarker(marker *string) []byte {
	if marker == nil || *marker == "" {
		return nil
	}
	return []byte(*marker)
}

// toAzureMetadata escapes metadata for Azure, which only accepts C# identifiers
// as metadata names and ASCII header values. Keys are hex-escaped to valid
// identifiers and values are URL-escaped; fromAzureMetadata reverses both. This
// mirrors gocloud.dev/blob/azureblob so metadata round-trips between the two.
func toAzureMetadata(metadata map[string]string) (map[string]*string, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	out := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		ek := escapeMetadataKey(k)
		if _, dup := out[ek]; dup {
			return nil, fmt.Errorf("%w: duplicate metadata key after escaping: %q", blobster.ErrInvalidOption, k)
		}
		ev := url.QueryEscape(v)
		out[ek] = &ev
	}
	return out, nil
}

func fromAzureMetadata(metadata map[string]*string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for k, v := range metadata {
		value := deref(v)
		if unescaped, err := url.QueryUnescape(value); err == nil {
			value = unescaped
		}
		out[strings.ToLower(unescapeKey(k))] = value
	}
	return out
}

func escapeMetadataKey(key string) string {
	return hexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		case i == 0 && c >= '0' && c <= '9':
			return true
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
			return false
		}
		return true
	})
}

// escapeKey hex-escapes the characters Azure Blob cannot address consistently,
// using the "__0x<hex>__" scheme of gocloud.dev/blob/azureblob so blob names
// round-trip between the two drivers. unescapeKey reverses it. isPrefix skips the
// trailing-slash rules that only matter for full object keys.
func escapeKey(key string, isPrefix bool) string {
	return hexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {
		// Azure does not handle backslashes in blob names well.
		case c == '\\':
			return true
		// Characters Azure cannot address consistently (control chars, " # % ? DEL).
		case c < 32 || c == '"' || c == '#' || c == '%' || c == '?' || c == 127:
			return true
		// A trailing "/" on a full key is not addressable; escape it.
		case !isPrefix && i == len(r)-1 && c == '/':
			return true
		// Escape the trailing slash in "../".
		case i > 1 && r[i] == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		}
		return false
	})
}

func unescapeKey(key string) string {
	return hexUnescape(key)
}

func hexEscape(s string, shouldEscape func(r []rune, i int) bool) string {
	rs := []rune(s)
	escape := false
	for i := range rs {
		if shouldEscape(rs, i) {
			escape = true
			break
		}
	}
	if !escape {
		return s
	}
	var b strings.Builder
	for i := range rs {
		if shouldEscape(rs, i) {
			fmt.Fprintf(&b, "__0x%x__", rs[i])
		} else {
			b.WriteRune(rs[i])
		}
	}
	return b.String()
}

func hexUnescape(s string) string {
	if !strings.Contains(s, "__0x") {
		return s
	}
	rs := []rune(s)
	var b strings.Builder
	for i := 0; i < len(rs); {
		if c, n, ok := unescapeRune(rs, i); ok {
			b.WriteRune(c)
			i += n
			continue
		}
		b.WriteRune(rs[i])
		i++
	}
	return b.String()
}

// unescapeRune decodes a "__0x<hex>__" sequence at rs[i], returning the decoded
// rune and the number of runes consumed.
func unescapeRune(rs []rune, i int) (rune, int, bool) {
	if i+4 >= len(rs) || rs[i] != '_' || rs[i+1] != '_' || rs[i+2] != '0' || rs[i+3] != 'x' {
		return 0, 0, false
	}
	j := i + 4
	for j < len(rs) && rs[j] != '_' {
		j++
	}
	if j == i+4 || j+1 >= len(rs) || rs[j] != '_' || rs[j+1] != '_' {
		return 0, 0, false
	}
	v, err := strconv.ParseInt(string(rs[i+4:j]), 16, 32)
	if err != nil {
		return 0, 0, false
	}
	return rune(v), (j + 2) - i, true
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	if bloberror.HasCode(err, bloberror.ConditionNotMet, bloberror.BlobAlreadyExists) {
		return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.StatusCode {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
		case http.StatusPreconditionFailed, http.StatusConflict:
			return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
		}
	}
	return err
}

func ptr[T any](v T) *T { return &v }

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}
