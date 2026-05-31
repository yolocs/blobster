package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/yolocs/blobster"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Bucket struct {
	backend backend
	prefix  string
}

type Option func(*options)

type options struct {
	prefix  string
	signing *SigningOptions
}

type SigningOptions struct {
	GoogleAccessID string
	PrivateKey     []byte
	SignBytes      func([]byte) ([]byte, error)
	Scheme         storage.SigningScheme
	Style          storage.URLStyle
	Hostname       string
	Insecure       bool
}

func WithPrefix(prefix string) Option {
	return func(opts *options) {
		opts.prefix = prefix
	}
}

func WithSignedURLs(signing SigningOptions) Option {
	return func(opts *options) {
		opts.signing = &signing
	}
}

func New(client *storage.Client, bucket string, optFns ...Option) *Bucket {
	var opts options
	for _, opt := range optFns {
		opt(&opts)
	}
	return &Bucket{
		backend: &storageBackend{
			client:   client,
			bucket:   bucket,
			signing:  opts.signing,
			rewriter: clientRewriter{client: client},
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

// XCopyFrom implements blobster.CrossRegionCopier. The source must be a gcs
// bucket (possibly in another region/project/account); the rewrite is issued
// against this (destination) bucket's client and names the source bucket, so the
// destination client's credential must have read access to the source.
// opts.BeforeCopy customizes the underlying storage.Copier.
func (b *Bucket) XCopyFrom(ctx context.Context, dstKey string, src blobster.Bucket, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error) {
	srcBucket, ok := src.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("%w: cross-region copy requires a gcs source bucket", blobster.ErrUnsupported)
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
	Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error
	Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error
	XCopyFrom(ctx context.Context, dstKey string, src backend, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error)
	ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error)
	IsAccessible(ctx context.Context) (bool, error)
	SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error)
	ErrorAs(err error, i any) bool
	Capabilities() blobster.Capabilities
}

type storageBackend struct {
	client   *storage.Client
	bucket   string
	signing  *SigningOptions
	rewriter rewriter
}

func (b *storageBackend) As(i any) bool {
	if blobster.AssignNative(b.client, i) {
		return true
	}
	return blobster.AssignNative(b.client.Bucket(b.bucket), i)
}

func (b *storageBackend) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	attrs, err := b.object(key).Attrs(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return convertAttrs(attrs), nil
}

func (b *storageBackend) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	if opts != nil && opts.BeforeRead != nil {
		obj := b.object(key)
		if err := opts.BeforeRead(func(i any) bool { return blobster.AssignNative(obj, i) }); err != nil {
			return nil, err
		}
	}
	reader := &gcsReader{
		ctx:    ctx,
		open:   b.openReader,
		key:    key,
		start:  offset,
		length: length,
	}
	if err := reader.reopen(0); err != nil {
		return nil, err
	}
	return reader, nil
}

func (b *storageBackend) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error) {
	obj := b.object(key)
	conds, ok, err := gcsConditions(preconditions)
	if err != nil {
		return nil, err
	}
	if ok {
		obj = obj.If(conds)
	}
	writer := obj.NewWriter(ctx)
	writer.CacheControl = opts.CacheControl
	writer.ContentDisposition = opts.ContentDisposition
	writer.ContentEncoding = opts.ContentEncoding
	writer.ContentLanguage = opts.ContentLanguage
	writer.ContentType = opts.ContentType
	writer.ForceEmptyContentType = opts.DisableContentTypeDetection
	writer.MD5 = bytes.Clone(opts.ContentMD5)
	writer.Metadata = blobster.CloneMetadata(opts.Metadata)
	if opts.BufferSize > 0 {
		writer.ChunkSize = opts.BufferSize
	}
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(i any) bool { return blobster.AssignNative(writer, i) }); err != nil {
			return nil, err
		}
	}
	return &writeCloser{w: writer}, nil
}

func (b *storageBackend) Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error {
	obj := b.object(key)
	conds, ok, err := gcsConditions(preconditions)
	if err != nil {
		return err
	}
	if ok {
		obj = obj.If(conds)
	}
	return mapError(obj.Delete(ctx))
}

func (b *storageBackend) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	copier := b.object(dstKey).CopierFrom(b.object(srcKey))
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(i any) bool { return blobster.AssignNative(copier, i) }); err != nil {
			return err
		}
	}
	_, err := copier.Run(ctx)
	return mapError(err)
}

func (b *storageBackend) XCopyFrom(ctx context.Context, dstKey string, src backend, srcKey string, opts *blobster.CopyOptions) (*blobster.CopyOperation, error) {
	srcBackend, ok := src.(*storageBackend)
	if !ok {
		return nil, fmt.Errorf("%w: cross-region copy requires a gcs source bucket", blobster.ErrUnsupported)
	}
	c := crossCopy{
		rewriter:  b.rewriter,
		dstBucket: b.bucket,
		dstKey:    dstKey,
		srcBucket: srcBackend.bucket,
		srcKey:    srcKey,
		opts:      opts,
	}
	return blobster.StartCopyOperation(ctx, c.run), nil
}

func (b *storageBackend) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	if len(pageToken) == 0 {
		return nil, nil, io.EOF
	}
	if opts == nil {
		opts = &blobster.ListOptions{}
	}
	query := &storage.Query{
		Prefix:    opts.Prefix,
		Delimiter: opts.Delimiter,
	}
	if opts.BeforeList != nil {
		if err := opts.BeforeList(func(i any) bool { return blobster.AssignNative(query, i) }); err != nil {
			return nil, nil, err
		}
	}
	token := ""
	if !bytes.Equal(pageToken, blobster.FirstPageToken) {
		token = string(pageToken)
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	iter := b.client.Bucket(b.bucket).Objects(ctx, query)
	var attrs []*storage.ObjectAttrs
	next, err := iterator.NewPager(iter, pageSize, token).NextPage(&attrs)
	if err != nil {
		return nil, nil, mapError(err)
	}
	page := make([]*blobster.ListObject, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Prefix != "" {
			page = append(page, &blobster.ListObject{
				Key:    attr.Prefix,
				IsDir:  true,
				Native: attr,
			})
			continue
		}
		page = append(page, &blobster.ListObject{
			Key:     attr.Name,
			ModTime: attr.Updated,
			Size:    attr.Size,
			MD5:     bytes.Clone(attr.MD5),
			Native:  attr,
		})
	}
	return page, []byte(next), nil
}

func (b *storageBackend) IsAccessible(ctx context.Context) (bool, error) {
	_, err := b.client.Bucket(b.bucket).Attrs(ctx)
	if err != nil {
		if errors.Is(mapError(err), blobster.ErrNotFound) {
			return false, nil
		}
		return false, mapError(err)
	}
	return true, nil
}

func (b *storageBackend) SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if b.signing == nil {
		return "", fmt.Errorf("%w: signed URLs require gcs.WithSignedURLs", blobster.ErrUnsupported)
	}
	if opts == nil {
		opts = &blobster.SignedURLOptions{}
	}
	expiry := opts.Expiry
	if expiry == 0 {
		expiry = blobster.DefaultSignedURLExpiry
	}
	method := opts.Method
	if method == "" {
		method = http.MethodGet
	}
	signedOptions := &storage.SignedURLOptions{
		GoogleAccessID: b.signing.GoogleAccessID,
		PrivateKey:     bytes.Clone(b.signing.PrivateKey),
		SignBytes:      b.signing.SignBytes,
		Method:         method,
		Expires:        time.Now().Add(expiry),
		ContentType:    opts.ContentType,
		Scheme:         b.signing.Scheme,
		Style:          b.signing.Style,
		Hostname:       b.signing.Hostname,
		Insecure:       b.signing.Insecure,
	}
	if opts.EnforceAbsentContentType {
		signedOptions.ContentType = ""
	}
	if opts.BeforeSign != nil {
		if err := opts.BeforeSign(func(i any) bool { return blobster.AssignNative(signedOptions, i) }); err != nil {
			return "", err
		}
	}
	return storage.SignedURL(b.bucket, key, signedOptions)
}

func (b *storageBackend) ErrorAs(err error, i any) bool {
	return errors.As(err, i)
}

func (b *storageBackend) Capabilities() blobster.Capabilities {
	return blobster.Capabilities{
		ConditionalWrites: true,
		Copy:              true,
		List:              true,
		ListPage:          true,
		RangeRead:         true,
		SignedURL:         b.signing != nil,
		CrossRegionCopy:   true,
	}
}

func (b *storageBackend) object(key string) *storage.ObjectHandle {
	return b.client.Bucket(b.bucket).Object(key)
}

func (b *storageBackend) openReader(ctx context.Context, key string, offset, length int64) (objectReader, *blobster.Attributes, error) {
	reader, err := b.object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, nil, mapError(err)
	}
	attrs := &blobster.Attributes{
		CacheControl:    reader.Attrs.CacheControl,
		ContentEncoding: reader.Attrs.ContentEncoding,
		ContentType:     reader.Attrs.ContentType,
		ModTime:         reader.Attrs.LastModified,
		Size:            reader.Attrs.Size,
		Version:         strconv.FormatInt(reader.Attrs.Generation, 10),
		Native:          reader,
	}
	return reader, attrs, nil
}

type objectReader interface {
	io.ReadCloser
	io.WriterTo
}

type writeCloser struct {
	w io.WriteCloser
}

func (w *writeCloser) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w *writeCloser) Close() error {
	return mapError(w.w.Close())
}

func (w *writeCloser) CloseWithError(err error) error {
	type closeWithError interface {
		CloseWithError(error) error
	}
	if aborter, ok := w.w.(closeWithError); ok {
		return mapError(aborter.CloseWithError(err))
	}
	return nil
}

func (w *writeCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(w.w, r)
}

// gcsReader implements Seek by closing the current network reader and opening a
// new GCS range reader. That preserves the Reader contract but is expensive for
// seek-heavy callers.
type gcsReader struct {
	ctx    context.Context
	open   func(context.Context, string, int64, int64) (objectReader, *blobster.Attributes, error)
	key    string
	start  int64
	length int64
	pos    int64
	reader objectReader
	attrs  *blobster.Attributes
	closed bool
}

func (r *gcsReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("blobster gcs: read after close")
	}
	n, err := r.reader.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *gcsReader) Seek(offset int64, whence int) (int64, error) {
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

func (r *gcsReader) Close() error {
	r.closed = true
	if r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

func (r *gcsReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, r)
}

func (r *gcsReader) As(i any) bool {
	if r.reader == nil {
		return false
	}
	return blobster.AssignNative(r.reader, i)
}

func (r *gcsReader) ContentType() string {
	return r.attrs.ContentType
}

func (r *gcsReader) ModTime() time.Time {
	return r.attrs.ModTime
}

func (r *gcsReader) Size() int64 {
	return r.attrs.Size
}

func (r *gcsReader) reopen(pos int64) error {
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

func (r *gcsReader) rangeSize() int64 {
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

func convertAttrs(attrs *storage.ObjectAttrs) *blobster.Attributes {
	if attrs == nil {
		return nil
	}
	return &blobster.Attributes{
		CacheControl:       attrs.CacheControl,
		ContentDisposition: attrs.ContentDisposition,
		ContentEncoding:    attrs.ContentEncoding,
		ContentLanguage:    attrs.ContentLanguage,
		ContentType:        attrs.ContentType,
		Metadata:           blobster.CloneMetadata(attrs.Metadata),
		CreateTime:         attrs.Created,
		ModTime:            attrs.Updated,
		Size:               attrs.Size,
		MD5:                bytes.Clone(attrs.MD5),
		Version:            strconv.FormatInt(attrs.Generation, 10),
		Native:             attrs,
	}
}

func gcsConditions(preconditions blobster.Preconditions) (storage.Conditions, bool, error) {
	switch {
	case preconditions.IfNotExists:
		return storage.Conditions{DoesNotExist: true}, true, nil
	case preconditions.IfMatch != "":
		generation, err := strconv.ParseInt(preconditions.IfMatch, 10, 64)
		if err != nil {
			return storage.Conditions{}, false, fmt.Errorf("%w: GCS version token must be a generation number", blobster.ErrInvalidOption)
		}
		return storage.Conditions{GenerationMatch: generation}, true, nil
	case preconditions.IfNotMatch != "":
		generation, err := strconv.ParseInt(preconditions.IfNotMatch, 10, 64)
		if err != nil {
			return storage.Conditions{}, false, fmt.Errorf("%w: GCS version token must be a generation number", blobster.ErrInvalidOption)
		}
		return storage.Conditions{GenerationNotMatch: generation}, true, nil
	default:
		return storage.Conditions{}, false, nil
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
	}
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	if code := status.Code(err); code == codes.FailedPrecondition || code == codes.Aborted {
		return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
	}
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	return err
}
