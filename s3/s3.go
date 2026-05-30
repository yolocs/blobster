// Package s3 implements a blobster.Bucket backed by AWS S3. It is built from a
// caller-owned *s3.Client (blobster never reads ambient AWS configuration and
// never closes the client). Conditional writes use S3's If-None-Match/If-Match
// support; large writes stream through the SDK's multipart upload manager.
package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/yolocs/blobster"
)

type Bucket struct {
	backend backend
	prefix  string
}

type Option func(*options)

type options struct {
	prefix string
}

func WithPrefix(prefix string) Option {
	return func(opts *options) {
		opts.prefix = prefix
	}
}

// New returns a Bucket backed by the named S3 bucket using the caller-owned
// client.
func New(client *s3.Client, bucket string, optFns ...Option) *Bucket {
	var opts options
	for _, opt := range optFns {
		opt(&opts)
	}
	return &Bucket{
		backend: &s3Backend{
			client:    client,
			bucket:    bucket,
			uploader:  manager.NewUploader(client),
			presigner: s3.NewPresignClient(client),
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
	ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error)
	IsAccessible(ctx context.Context) (bool, error)
	SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error)
	ErrorAs(err error, i any) bool
	Capabilities() blobster.Capabilities
}

type s3Backend struct {
	client    *s3.Client
	bucket    string
	uploader  *manager.Uploader
	presigner *s3.PresignClient
}

func (b *s3Backend) As(i any) bool {
	return blobster.AssignNative(b.client, i)
}

func (b *s3Backend) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &blobster.Attributes{
		CacheControl:       aws.ToString(out.CacheControl),
		ContentDisposition: aws.ToString(out.ContentDisposition),
		ContentEncoding:    aws.ToString(out.ContentEncoding),
		ContentLanguage:    aws.ToString(out.ContentLanguage),
		ContentType:        aws.ToString(out.ContentType),
		Metadata:           blobster.CloneMetadata(out.Metadata),
		ModTime:            aws.ToTime(out.LastModified),
		Size:               aws.ToInt64(out.ContentLength),
		Version:            etagVersion(out.ETag),
		Native:             out,
	}, nil
}

func (b *s3Backend) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	if opts != nil && opts.BeforeRead != nil {
		input := &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)}
		if err := opts.BeforeRead(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return nil, err
		}
	}
	r := &s3Reader{
		ctx:    ctx,
		open:   b.openReader,
		key:    key,
		start:  offset,
		length: length,
	}
	if err := r.reopen(0); err != nil {
		return nil, err
	}
	return r, nil
}

func (b *s3Backend) openReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, *blobster.Attributes, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if rng := rangeHeader(offset, length); rng != "" {
		input.Range = aws.String(rng)
	}
	out, err := b.client.GetObject(ctx, input)
	if err != nil {
		return nil, nil, mapError(err)
	}
	size := aws.ToInt64(out.ContentLength)
	if total, ok := totalFromContentRange(out.ContentRange); ok {
		size = total
	}
	attrs := &blobster.Attributes{
		CacheControl:    aws.ToString(out.CacheControl),
		ContentEncoding: aws.ToString(out.ContentEncoding),
		ContentType:     aws.ToString(out.ContentType),
		Metadata:        blobster.CloneMetadata(out.Metadata),
		ModTime:         aws.ToTime(out.LastModified),
		Size:            size,
		Version:         etagVersion(out.ETag),
		Native:          out,
	}
	return out.Body, attrs, nil
}

func (b *s3Backend) NewWriter(ctx context.Context, key string, opts *blobster.WriterOptions, preconditions blobster.Preconditions) (blobster.Writer, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if opts.CacheControl != "" {
		input.CacheControl = aws.String(opts.CacheControl)
	}
	if opts.ContentDisposition != "" {
		input.ContentDisposition = aws.String(opts.ContentDisposition)
	}
	if opts.ContentEncoding != "" {
		input.ContentEncoding = aws.String(opts.ContentEncoding)
	}
	if opts.ContentLanguage != "" {
		input.ContentLanguage = aws.String(opts.ContentLanguage)
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	if len(opts.Metadata) > 0 {
		input.Metadata = blobster.CloneMetadata(opts.Metadata)
	}
	if len(opts.ContentMD5) > 0 {
		input.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString(opts.ContentMD5))
	}
	if err := applyWriteConditions(input, preconditions); err != nil {
		return nil, err
	}
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return nil, err
		}
	}

	pr, pw := io.Pipe()
	input.Body = pr
	w := &s3Writer{
		pw:   pw,
		done: make(chan struct{}),
	}
	uploadOpts := []func(*manager.Uploader){}
	if opts.BufferSize > 0 {
		uploadOpts = append(uploadOpts, func(u *manager.Uploader) { u.PartSize = int64(opts.BufferSize) })
	}
	if opts.MaxConcurrency > 0 {
		uploadOpts = append(uploadOpts, func(u *manager.Uploader) { u.Concurrency = opts.MaxConcurrency })
	}
	go func() {
		_, err := b.uploader.Upload(ctx, input, uploadOpts...)
		w.err = mapError(err)
		// Unblock any in-flight Write once the upload has stopped consuming.
		pr.CloseWithError(err)
		close(w.done)
	}()
	return w, nil
}

func (b *s3Backend) Delete(ctx context.Context, key string, preconditions blobster.Preconditions) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	switch {
	case preconditions.IfMatch != "":
		input.IfMatch = aws.String(etagHeader(preconditions.IfMatch))
	case preconditions.IfNotExists, preconditions.IfNotMatch != "":
		return fmt.Errorf("%w: S3 conditional delete supports only IfMatch", blobster.ErrUnsupported)
	}
	// S3 DeleteObject is idempotent and returns no error for a missing key, so
	// confirm existence first to honor the ErrNotFound contract.
	if _, err := b.Attributes(ctx, key); err != nil {
		return err
	}
	if _, err := b.client.DeleteObject(ctx, input); err != nil {
		return mapError(err)
	}
	return nil
}

func (b *s3Backend) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(copySource(b.bucket, srcKey)),
	}
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return err
		}
	}
	if _, err := b.client.CopyObject(ctx, input); err != nil {
		return mapError(err)
	}
	return nil
}

func (b *s3Backend) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	if len(pageToken) == 0 {
		return nil, nil, io.EOF
	}
	if opts == nil {
		opts = &blobster.ListOptions{}
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.bucket),
		MaxKeys: aws.Int32(int32(pageSize)),
	}
	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}
	if opts.Delimiter != "" {
		input.Delimiter = aws.String(opts.Delimiter)
	}
	if !bytes.Equal(pageToken, blobster.FirstPageToken) {
		input.ContinuationToken = aws.String(string(pageToken))
	}
	if opts.BeforeList != nil {
		if err := opts.BeforeList(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return nil, nil, err
		}
	}
	out, err := b.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, nil, mapError(err)
	}
	page := make([]*blobster.ListObject, 0, len(out.Contents)+len(out.CommonPrefixes))
	for _, prefix := range out.CommonPrefixes {
		page = append(page, &blobster.ListObject{
			Key:    aws.ToString(prefix.Prefix),
			IsDir:  true,
			Native: prefix,
		})
	}
	for _, obj := range out.Contents {
		page = append(page, &blobster.ListObject{
			Key:     aws.ToString(obj.Key),
			ModTime: aws.ToTime(obj.LastModified),
			Size:    aws.ToInt64(obj.Size),
			Native:  obj,
		})
	}
	var next []byte
	if aws.ToBool(out.IsTruncated) {
		next = []byte(aws.ToString(out.NextContinuationToken))
	}
	return page, next, nil
}

func (b *s3Backend) IsAccessible(ctx context.Context) (bool, error) {
	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(b.bucket)})
	if err != nil {
		if errors.Is(mapError(err), blobster.ErrNotFound) {
			return false, nil
		}
		return false, mapError(err)
	}
	return true, nil
}

func (b *s3Backend) SignedURL(ctx context.Context, key string, opts *blobster.SignedURLOptions) (string, error) {
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
	expires := func(o *s3.PresignOptions) { o.Expires = expiry }

	contentType := opts.ContentType
	if opts.EnforceAbsentContentType {
		contentType = ""
	}

	switch strings.ToUpper(method) {
	case http.MethodGet:
		signed, err := b.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		}, expires)
		if err != nil {
			return "", mapError(err)
		}
		return signed.URL, nil
	case http.MethodPut:
		in := &s3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)}
		if contentType != "" {
			in.ContentType = aws.String(contentType)
		}
		signed, err := b.presigner.PresignPutObject(ctx, in, expires)
		if err != nil {
			return "", mapError(err)
		}
		return signed.URL, nil
	case http.MethodDelete:
		signed, err := b.presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		}, expires)
		if err != nil {
			return "", mapError(err)
		}
		return signed.URL, nil
	default:
		return "", fmt.Errorf("%w: unsupported signed URL method %q", blobster.ErrInvalidOption, method)
	}
}

func (b *s3Backend) ErrorAs(err error, i any) bool {
	return errors.As(err, i)
}

func (b *s3Backend) Capabilities() blobster.Capabilities {
	return blobster.Capabilities{
		ConditionalWrites: true,
		Copy:              true,
		List:              true,
		ListPage:          true,
		RangeRead:         true,
		SignedURL:         true,
	}
}

func applyWriteConditions(input *s3.PutObjectInput, preconditions blobster.Preconditions) error {
	switch {
	case preconditions.IfNotExists:
		input.IfNoneMatch = aws.String("*")
	case preconditions.IfMatch != "":
		input.IfMatch = aws.String(etagHeader(preconditions.IfMatch))
	case preconditions.IfNotMatch != "":
		input.IfNoneMatch = aws.String(etagHeader(preconditions.IfNotMatch))
	}
	return nil
}

// s3Reader implements Seek by closing the current GetObject body and reopening a
// new range request, mirroring the GCS reader. Seek-heavy callers pay for a new
// request per seek.
type s3Reader struct {
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

func (r *s3Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("blobster s3: read after close")
	}
	// Clamp to the bytes remaining in the requested range. This bounds an empty
	// range (e.g. length 0, or a seek to the end) to zero bytes regardless of
	// what S3 returns, and defends against a response exceeding the upper bound.
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

func (r *s3Reader) Seek(offset int64, whence int) (int64, error) {
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

func (r *s3Reader) Close() error {
	r.closed = true
	if r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

func (r *s3Reader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, struct{ io.Reader }{r})
}

func (r *s3Reader) As(i any) bool {
	if r.reader == nil {
		return false
	}
	return blobster.AssignNative(r.reader, i)
}

func (r *s3Reader) ContentType() string {
	return r.attrs.ContentType
}

func (r *s3Reader) ModTime() time.Time {
	return r.attrs.ModTime
}

func (r *s3Reader) Size() int64 {
	return r.attrs.Size
}

func (r *s3Reader) reopen(pos int64) error {
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
	// S3 has no zero-length range request (it would 416). When we already know
	// the object's size, a seek that lands an empty bounded range serves an
	// empty body directly rather than issuing an invalid request.
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

func (r *s3Reader) rangeSize() int64 {
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

type s3Writer struct {
	pw     *io.PipeWriter
	done   chan struct{}
	err    error
	closed bool
}

func (w *s3Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("blobster s3: write after close")
	}
	return w.pw.Write(p)
}

func (w *s3Writer) ReadFrom(r io.Reader) (int64, error) {
	if w.closed {
		return 0, errors.New("blobster s3: read from after close")
	}
	return io.Copy(w.pw, r)
}

func (w *s3Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.done
	return w.err
}

func (w *s3Writer) CloseWithError(cause error) error {
	if w.closed {
		return nil
	}
	w.closed = true
	if cause == nil {
		cause = errors.New("blobster s3: write aborted")
	}
	w.pw.CloseWithError(cause)
	<-w.done
	return nil
}

func rangeHeader(offset, length int64) string {
	if offset <= 0 && length < 0 {
		return ""
	}
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	if length == 0 {
		// Request a single byte then discard; S3 has no zero-length range. The
		// caller reads nothing because rangeSize bounds Read at zero.
		return fmt.Sprintf("bytes=%d-%d", offset, offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
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

func copySource(bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return url.PathEscape(bucket) + "/" + strings.Join(segments, "/")
}

func etagVersion(etag *string) string {
	return strings.Trim(aws.ToString(etag), `"`)
}

func etagHeader(version string) string {
	if version == "" {
		return version
	}
	if strings.HasPrefix(version, `"`) {
		return version
	}
	return `"` + version + `"`
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict":
			return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
		}
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %w", blobster.ErrNotFound, err)
		case http.StatusPreconditionFailed, http.StatusConflict:
			return fmt.Errorf("%w: %w", blobster.ErrPreconditionFailed, err)
		}
	}
	return err
}
