package mem

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yolocs/blobster"
)

type Bucket struct {
	state  *state
	prefix string
}

type state struct {
	mu          sync.Mutex
	objects     map[string]object
	nextVersion uint64
}

type object struct {
	data  []byte
	attrs *blobster.Attributes
}

func New() *Bucket {
	return &Bucket{
		state: &state{
			objects: make(map[string]object),
		},
	}
}

func (b *Bucket) As(i any) bool {
	return false
}

func (b *Bucket) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	obj, ok := b.state.objects[b.objectKey(key)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}
	return obj.attrs.Clone(), nil
}

func (b *Bucket) Close() error {
	return nil
}

func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions, preconditions ...blobster.Precondition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(any) bool { return false }); err != nil {
			return err
		}
	}
	compiled, err := blobster.CompilePreconditions(preconditions)
	if err != nil {
		return err
	}

	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	src, ok := b.state.objects[b.objectKey(srcKey)]
	if !ok {
		return fmt.Errorf("%w: %s", blobster.ErrNotFound, srcKey)
	}
	dst := b.objectKey(dstKey)
	current, exists := b.state.objects[dst]
	if !conditionsPass(compiled, current.attrs, exists) {
		return fmt.Errorf("%w: %s", blobster.ErrPreconditionFailed, dstKey)
	}

	now := time.Now()
	b.state.nextVersion++
	attrs := src.attrs.Clone()
	attrs.CreateTime = now
	attrs.ModTime = now
	attrs.Version = strconv.FormatUint(b.state.nextVersion, 10)
	attrs.Native = nil
	b.state.objects[dst] = object{
		data:  bytes.Clone(src.data),
		attrs: attrs,
	}
	return nil
}

func (b *Bucket) Delete(ctx context.Context, key string, preconditions ...blobster.Precondition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	compiled, err := blobster.CompilePreconditions(preconditions)
	if err != nil {
		return err
	}

	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	fullKey := b.objectKey(key)
	current, exists := b.state.objects[fullKey]
	if !exists {
		return fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}
	if !conditionsPass(compiled, current.attrs, true) {
		return fmt.Errorf("%w: %s", blobster.ErrPreconditionFailed, key)
	}
	delete(b.state.objects, fullKey)
	return nil
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
	return errors.As(err, i)
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
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bucket) List(opts *blobster.ListOptions) *blobster.ListIterator {
	return blobster.NewListIterator(b, opts)
}

func (b *Bucket) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *blobster.ListOptions) ([]*blobster.ListObject, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(pageToken) == 0 {
		return nil, nil, io.EOF
	}
	start, err := decodePageToken(pageToken)
	if err != nil {
		return nil, nil, err
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	if opts == nil {
		opts = &blobster.ListOptions{}
	}
	if opts.BeforeList != nil {
		if err := opts.BeforeList(func(any) bool { return false }); err != nil {
			return nil, nil, err
		}
	}

	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	results := b.listObjectsLocked(opts)
	if start >= len(results) {
		return nil, nil, nil
	}
	end := min(start+pageSize, len(results))
	page := results[start:end]
	var next []byte
	if end < len(results) {
		next = []byte(strconv.Itoa(end))
	}
	return page, next, nil
}

func (b *Bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blobster.ReaderOptions) (blobster.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts != nil && opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}
	if offset < 0 {
		return nil, fmt.Errorf("%w: range offset cannot be negative", blobster.ErrInvalidOption)
	}
	b.state.mu.Lock()
	obj, ok := b.state.objects[b.objectKey(key)]
	b.state.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}

	data := bytes.Clone(obj.data)
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	end := int64(len(data))
	if length >= 0 && offset+length < end {
		end = offset + length
	}
	return &reader{
		Reader: bytes.NewReader(data[offset:end]),
		attrs:  obj.attrs.Clone(),
	}, nil
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
	if cloned.BeforeWrite != nil {
		if err := cloned.BeforeWrite(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}
	return &writer{
		ctx:           ctx,
		bucket:        b,
		key:           key,
		opts:          cloned,
		preconditions: compiled,
	}, nil
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
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%w: signed URLs are not available for mem buckets", blobster.ErrUnsupported)
}

func (b *Bucket) Sub(prefix string) blobster.Bucket {
	return &Bucket{
		state:  b.state,
		prefix: b.prefix + prefix,
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
	return blobster.Capabilities{
		ConditionalWrites: true,
		Copy:              true,
		List:              true,
		ListPage:          true,
		RangeRead:         true,
	}
}

func (b *Bucket) objectKey(key string) string {
	return b.prefix + key
}

func (b *Bucket) listObjectsLocked(opts *blobster.ListOptions) []*blobster.ListObject {
	fullPrefix := b.objectKey(opts.Prefix)
	var keys []string
	for key := range b.state.objects {
		if strings.HasPrefix(key, fullPrefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)

	var results []*blobster.ListObject
	seenDirs := map[string]bool{}
	for _, fullKey := range keys {
		obj := b.state.objects[fullKey]
		relativeKey := strings.TrimPrefix(fullKey, b.prefix)
		if opts.Delimiter != "" {
			rest := strings.TrimPrefix(relativeKey, opts.Prefix)
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				dirKey := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if !seenDirs[dirKey] {
					seenDirs[dirKey] = true
					results = append(results, &blobster.ListObject{Key: dirKey, IsDir: true})
				}
				continue
			}
		}
		results = append(results, &blobster.ListObject{
			Key:     relativeKey,
			ModTime: obj.attrs.ModTime,
			Size:    obj.attrs.Size,
			MD5:     bytes.Clone(obj.attrs.MD5),
		})
	}
	return results
}

type reader struct {
	*bytes.Reader
	attrs *blobster.Attributes
}

func (r *reader) As(i any) bool {
	return r.attrs.As(i)
}

func (r *reader) Close() error {
	return nil
}

func (r *reader) ContentType() string {
	return r.attrs.ContentType
}

func (r *reader) ModTime() time.Time {
	return r.attrs.ModTime
}

func (r *reader) Size() int64 {
	return r.attrs.Size
}

type writer struct {
	ctx           context.Context
	bucket        *Bucket
	key           string
	opts          *blobster.WriterOptions
	preconditions blobster.Preconditions
	buf           bytes.Buffer
	closed        bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("blobster mem: write after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.buf.Write(p)
}

func (w *writer) ReadFrom(r io.Reader) (int64, error) {
	if w.closed {
		return 0, errors.New("blobster mem: read from after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return io.Copy(&w.buf, r)
}

func (w *writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.ctx.Err(); err != nil {
		return err
	}
	data := bytes.Clone(w.buf.Bytes())
	if len(w.opts.ContentMD5) > 0 {
		sum := md5.Sum(data)
		if !bytes.Equal(sum[:], w.opts.ContentMD5) {
			return fmt.Errorf("%w: content md5 mismatch", blobster.ErrInvalidOption)
		}
	}

	w.bucket.state.mu.Lock()
	defer w.bucket.state.mu.Unlock()

	fullKey := w.bucket.objectKey(w.key)
	current, exists := w.bucket.state.objects[fullKey]
	if !conditionsPass(w.preconditions, current.attrs, exists) {
		return fmt.Errorf("%w: %s", blobster.ErrPreconditionFailed, w.key)
	}

	now := time.Now()
	w.bucket.state.nextVersion++
	contentMD5 := md5.Sum(data)
	attrs := &blobster.Attributes{
		CacheControl:       w.opts.CacheControl,
		ContentDisposition: w.opts.ContentDisposition,
		ContentEncoding:    w.opts.ContentEncoding,
		ContentLanguage:    w.opts.ContentLanguage,
		ContentType:        w.opts.ContentType,
		Metadata:           blobster.CloneMetadata(w.opts.Metadata),
		CreateTime:         now,
		ModTime:            now,
		Size:               int64(len(data)),
		MD5:                bytes.Clone(contentMD5[:]),
		Version:            strconv.FormatUint(w.bucket.state.nextVersion, 10),
	}
	w.bucket.state.objects[fullKey] = object{data: data, attrs: attrs}
	return nil
}

func (w *writer) CloseWithError(error) error {
	w.closed = true
	return nil
}

func conditionsPass(preconditions blobster.Preconditions, attrs *blobster.Attributes, exists bool) bool {
	switch {
	case preconditions.IfNotExists:
		return !exists
	case preconditions.IfMatch != "":
		return exists && attrs != nil && attrs.Version == preconditions.IfMatch
	case preconditions.IfNotMatch != "":
		return !exists || attrs == nil || attrs.Version != preconditions.IfNotMatch
	default:
		return true
	}
}

func decodePageToken(token []byte) (int, error) {
	if bytes.Equal(token, blobster.FirstPageToken) {
		return 0, nil
	}
	offset, err := strconv.Atoi(string(token))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: invalid page token", blobster.ErrInvalidOption)
	}
	return offset, nil
}
