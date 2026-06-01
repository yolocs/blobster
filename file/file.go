// Package file implements a filesystem-backed blobster.Bucket. It is a
// first-class driver (not a test double): it provides the same conditional-write
// semantics as the cloud drivers, so it is suitable for local integration and
// small single-host deployments.
//
// Storage is split into three sibling subtrees under the bucket root: caller
// objects live under "data/<key>", a JSON metadata sidecar (content headers,
// user metadata, MD5, size, and an opaque version token) lives under
// "meta/<key>", and in-flight temp files live under "tmp/". Keeping metadata and
// temps in separate trees means no caller key can ever collide with bookkeeping
// state. Writes are atomic (write-to-temp + rename) and conditional
// writes/deletes are serialized through a per-bucket lock so create-only and
// compare-and-swap have exactly one winner; readers take the same lock while
// reading committed state so they never observe a half-applied commit.
package file

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yolocs/blobster"
)

const (
	dataSubdir = "data"
	metaSubdir = "meta"
	tempSubdir = "tmp"
)

const tempPattern = "w-*"

type Bucket struct {
	root   string
	prefix string
	mu     *sync.Mutex
}

// New returns a Bucket rooted at dir. The directory must already exist; keys are
// mapped to paths beneath it. The caller owns the directory's lifecycle.
func New(dir string) *Bucket {
	return &Bucket{
		root: dir,
		mu:   &sync.Mutex{},
	}
}

func (b *Bucket) As(i any) bool {
	return false
}

func (b *Bucket) Attributes(ctx context.Context, key string) (*blobster.Attributes, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := b.paths(key)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return b.readAttributes(p)
}

func (b *Bucket) Close() error {
	return nil
}

func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *blobster.CopyOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(any) bool { return false }); err != nil {
			return err
		}
	}
	src, err := b.paths(srcKey)
	if err != nil {
		return err
	}
	dst, err := b.paths(dstKey)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := os.ReadFile(src.data)
	if err != nil {
		return mapError(err, srcKey)
	}
	srcAttrs, err := b.readAttributes(src)
	if err != nil {
		return err
	}

	now := time.Now()
	attrs := srcAttrs.Clone()
	attrs.Native = nil
	attrs.CreateTime = now
	attrs.ModTime = now
	attrs.Version = newVersion()
	return b.commit(dst, data, attrs)
}

func (b *Bucket) Delete(ctx context.Context, key string, preconditions ...blobster.Precondition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	compiled, err := blobster.CompilePreconditions(preconditions)
	if err != nil {
		return err
	}
	p, err := b.paths(key)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	current, exists, err := b.statVersion(p)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}
	if !conditionsPass(compiled, current, true) {
		return fmt.Errorf("%w: %s", blobster.ErrPreconditionFailed, key)
	}
	if err := os.Remove(p.data); err != nil {
		return mapError(err, key)
	}
	_ = os.Remove(p.meta)
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
	info, err := os.Stat(b.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
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

	b.mu.Lock()
	defer b.mu.Unlock()

	results, err := b.listObjects(opts)
	if err != nil {
		return nil, nil, err
	}
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
	p, err := b.paths(key)
	if err != nil {
		return nil, err
	}

	// Hold the lock only while reading committed state and opening the file
	// descriptor; the open fd then streams independently of later renames.
	b.mu.Lock()
	attrs, err := b.readAttributes(p)
	if err != nil {
		b.mu.Unlock()
		return nil, err
	}
	f, err := os.Open(p.data)
	b.mu.Unlock()
	if err != nil {
		return nil, mapError(err, key)
	}

	size := attrs.Size
	if offset > size {
		offset = size
	}
	readLen := size - offset
	if length >= 0 && length < readLen {
		readLen = length
	}
	return &reader{
		f:       f,
		section: io.NewSectionReader(f, offset, readLen),
		attrs:   attrs,
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
	p, err := b.paths(key)
	if err != nil {
		return nil, err
	}
	tmp, err := b.newTemp()
	if err != nil {
		return nil, err
	}
	return &writer{
		ctx:           ctx,
		bucket:        b,
		key:           key,
		paths:         p,
		tmp:           tmp,
		opts:          cloned,
		preconditions: compiled,
		hash:          md5.New(),
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
	return "", fmt.Errorf("%w: signed URLs are not available for file buckets", blobster.ErrUnsupported)
}

func (b *Bucket) Sub(prefix string) blobster.Bucket {
	return &Bucket{
		root:   b.root,
		prefix: b.prefix + prefix,
		mu:     b.mu,
	}
}

func (b *Bucket) UpdateMetadata(ctx context.Context, key string, md map[string]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	normalized, err := blobster.NormalizeMetadata(md)
	if err != nil {
		return "", err
	}
	p, err := b.paths(key)
	if err != nil {
		return "", err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	current, exists, err := b.statVersion(p)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}

	// Rewrite only the metadata sidecar; the data file (existence and body source
	// of truth) is left untouched, so the body and its ModTime are preserved.
	attrs := current.Clone()
	attrs.Metadata = blobster.CloneMetadata(normalized)
	attrs.Version = newVersion()
	if err := b.writeFileAtomic(p.meta, mustMarshalAttrs(attrs)); err != nil {
		return "", err
	}
	return attrs.Version, nil
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

// objectPaths is the data and metadata file locations for one object.
type objectPaths struct {
	data string
	meta string
}

// paths maps a caller key to its data and metadata file paths, rejecting keys
// that would escape the bucket's data/meta subtrees.
func (b *Bucket) paths(key string) (objectPaths, error) {
	full := b.objectKey(key)
	if full == "" {
		return objectPaths{}, fmt.Errorf("%w: empty key", blobster.ErrInvalidOption)
	}
	rel := filepath.FromSlash(full)
	data, err := securejoin(filepath.Join(b.root, dataSubdir), rel)
	if err != nil {
		return objectPaths{}, fmt.Errorf("%w: %s", err, key)
	}
	meta, err := securejoin(filepath.Join(b.root, metaSubdir), rel)
	if err != nil {
		return objectPaths{}, fmt.Errorf("%w: %s", err, key)
	}
	return objectPaths{data: data, meta: meta}, nil
}

// securejoin joins rel onto base and verifies the result stays within base.
func securejoin(base, rel string) (string, error) {
	clean := filepath.Clean(filepath.Join(base, rel))
	baseClean := filepath.Clean(base)
	if clean != baseClean && !strings.HasPrefix(clean, baseClean+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: key escapes bucket root", blobster.ErrInvalidOption)
	}
	if clean == baseClean {
		return "", fmt.Errorf("%w: key resolves to bucket root", blobster.ErrInvalidOption)
	}
	return clean, nil
}

func (b *Bucket) newTemp() (*os.File, error) {
	dir := filepath.Join(b.root, tempSubdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, tempPattern)
}

// commit atomically writes data then its metadata sidecar. Data is the source of
// truth for existence, so it is renamed first. It must be called holding b.mu.
func (b *Bucket) commit(p objectPaths, data []byte, attrs *blobster.Attributes) error {
	if err := os.MkdirAll(filepath.Dir(p.data), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p.meta), 0o755); err != nil {
		return err
	}
	if err := b.writeFileAtomic(p.data, data); err != nil {
		return err
	}
	if err := b.writeFileAtomic(p.meta, mustMarshalAttrs(attrs)); err != nil {
		return err
	}
	return nil
}

func (b *Bucket) readAttributes(p objectPaths) (*blobster.Attributes, error) {
	info, err := os.Stat(p.data)
	if err != nil {
		return nil, mapError(err, p.data)
	}
	attrs := &blobster.Attributes{
		ModTime: info.ModTime(),
		Size:    info.Size(),
	}
	sidecar, err := os.ReadFile(p.meta)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		// Tolerate a data file with no sidecar (e.g. mid-commit or created out of
		// band): derive a version from the data file so reads still work.
		attrs.Version = deriveVersion(info)
		return attrs, nil
	}
	var stored storedAttrs
	if err := json.Unmarshal(sidecar, &stored); err != nil {
		return nil, err
	}
	attrs.CacheControl = stored.CacheControl
	attrs.ContentDisposition = stored.ContentDisposition
	attrs.ContentEncoding = stored.ContentEncoding
	attrs.ContentLanguage = stored.ContentLanguage
	attrs.ContentType = stored.ContentType
	attrs.Metadata = blobster.CloneMetadata(stored.Metadata)
	attrs.CreateTime = stored.CreateTime
	attrs.MD5 = bytes.Clone(stored.MD5)
	attrs.Version = stored.Version
	if stored.Size > 0 || stored.Version != "" {
		attrs.Size = stored.Size
	}
	return attrs, nil
}

// statVersion returns the current committed attributes of an object, if it
// exists. It must be called holding b.mu.
func (b *Bucket) statVersion(p objectPaths) (*blobster.Attributes, bool, error) {
	attrs, err := b.readAttributes(p)
	if err != nil {
		if errors.Is(err, blobster.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return attrs, true, nil
}

// listObjects walks the data subtree. Caller keys live exclusively under that
// subtree, so there is no bookkeeping state to filter out. It must be called
// holding b.mu.
func (b *Bucket) listObjects(opts *blobster.ListOptions) ([]*blobster.ListObject, error) {
	dataRoot := filepath.Clean(filepath.Join(b.root, dataSubdir))
	type entry struct {
		key  string
		info os.FileInfo
	}
	var entries []entry
	err := filepath.Walk(dataRoot, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataRoot, p)
		if err != nil {
			return err
		}
		entries = append(entries, entry{key: filepath.ToSlash(rel), info: info})
		return nil
	})
	if err != nil {
		return nil, err
	}

	fullPrefix := b.objectKey(opts.Prefix)
	var keys []string
	infos := map[string]os.FileInfo{}
	for _, e := range entries {
		if strings.HasPrefix(e.key, fullPrefix) {
			keys = append(keys, e.key)
			infos[e.key] = e.info
		}
	}
	slices.Sort(keys)

	var results []*blobster.ListObject
	seenDirs := map[string]bool{}
	for _, fullKey := range keys {
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
		info := infos[fullKey]
		var md5sum []byte
		if attrs, err := b.readAttributes(objectPaths{
			data: filepath.Join(dataRoot, filepath.FromSlash(fullKey)),
			meta: filepath.Join(filepath.Clean(filepath.Join(b.root, metaSubdir)), filepath.FromSlash(fullKey)),
		}); err == nil {
			md5sum = bytes.Clone(attrs.MD5)
		}
		results = append(results, &blobster.ListObject{
			Key:     relativeKey,
			ModTime: info.ModTime(),
			Size:    info.Size(),
			MD5:     md5sum,
		})
	}
	return results, nil
}

type storedAttrs struct {
	Version            string            `json:"version"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	ContentLanguage    string            `json:"content_language,omitempty"`
	ContentType        string            `json:"content_type,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	MD5                []byte            `json:"md5,omitempty"`
	Size               int64             `json:"size"`
	CreateTime         time.Time         `json:"create_time"`
}

func mustMarshalAttrs(attrs *blobster.Attributes) []byte {
	stored := storedAttrs{
		Version:            attrs.Version,
		CacheControl:       attrs.CacheControl,
		ContentDisposition: attrs.ContentDisposition,
		ContentEncoding:    attrs.ContentEncoding,
		ContentLanguage:    attrs.ContentLanguage,
		ContentType:        attrs.ContentType,
		Metadata:           attrs.Metadata,
		MD5:                attrs.MD5,
		Size:               attrs.Size,
		CreateTime:         attrs.CreateTime,
	}
	data, _ := json.Marshal(stored)
	return data
}

type reader struct {
	f       *os.File
	section *io.SectionReader
	attrs   *blobster.Attributes
}

func (r *reader) Read(p []byte) (int, error) {
	return r.section.Read(p)
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	return r.section.Seek(offset, whence)
}

func (r *reader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, r.section)
}

func (r *reader) Close() error {
	return r.f.Close()
}

func (r *reader) As(i any) bool {
	return r.attrs.As(i)
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
	paths         objectPaths
	tmp           *os.File
	opts          *blobster.WriterOptions
	preconditions blobster.Preconditions
	hash          interface {
		io.Writer
		Sum([]byte) []byte
	}
	size   int64
	closed bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("blobster file: write after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.tmp.Write(p)
	if n > 0 {
		w.hash.Write(p[:n])
		w.size += int64(n)
	}
	return n, err
}

func (w *writer) ReadFrom(r io.Reader) (int64, error) {
	if w.closed {
		return 0, errors.New("blobster file: read from after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	tee := io.TeeReader(r, w.hash)
	n, err := io.Copy(w.tmp, tee)
	w.size += n
	return n, err
}

func (w *writer) Close() (retErr error) {
	if w.closed {
		return nil
	}
	w.closed = true

	tmpName := w.tmp.Name()
	committed := false
	defer func() {
		if !committed {
			os.Remove(tmpName)
		}
	}()

	if err := w.ctx.Err(); err != nil {
		w.tmp.Close()
		return err
	}
	sum := w.hash.Sum(nil)
	if len(w.opts.ContentMD5) > 0 && !bytes.Equal(sum, w.opts.ContentMD5) {
		w.tmp.Close()
		return fmt.Errorf("%w: content md5 mismatch", blobster.ErrInvalidOption)
	}
	if err := w.tmp.Sync(); err != nil {
		w.tmp.Close()
		return err
	}
	if err := w.tmp.Close(); err != nil {
		return err
	}

	now := time.Now()
	attrs := &blobster.Attributes{
		CacheControl:       w.opts.CacheControl,
		ContentDisposition: w.opts.ContentDisposition,
		ContentEncoding:    w.opts.ContentEncoding,
		ContentLanguage:    w.opts.ContentLanguage,
		ContentType:        w.opts.ContentType,
		Metadata:           blobster.CloneMetadata(w.opts.Metadata),
		CreateTime:         now,
		ModTime:            now,
		Size:               w.size,
		MD5:                sum,
		Version:            newVersion(),
	}

	w.bucket.mu.Lock()
	defer w.bucket.mu.Unlock()

	current, exists, err := w.bucket.statVersion(w.paths)
	if err != nil {
		return err
	}
	if !conditionsPass(w.preconditions, current, exists) {
		return fmt.Errorf("%w: %s", blobster.ErrPreconditionFailed, w.key)
	}

	if err := os.MkdirAll(filepath.Dir(w.paths.data), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(w.paths.meta), 0o755); err != nil {
		return err
	}
	// Rename data into place first (it is the existence source of truth), then
	// the metadata sidecar.
	if err := os.Rename(tmpName, w.paths.data); err != nil {
		return err
	}
	committed = true
	if err := w.bucket.writeFileAtomic(w.paths.meta, mustMarshalAttrs(attrs)); err != nil {
		return err
	}
	return nil
}

func (w *writer) CloseWithError(error) error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.tmp.Close()
	os.Remove(w.tmp.Name())
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

// writeFileAtomic writes data to a temp file in the bucket's temp dir and renames
// it onto path. Both live under the bucket root, so the rename is atomic.
func (b *Bucket) writeFileAtomic(path string, data []byte) error {
	tmp, err := b.newTemp()
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func newVersion() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(buf[:])
}

func deriveVersion(info os.FileInfo) string {
	return strconv.FormatInt(info.ModTime().UnixNano(), 10) + "-" + strconv.FormatInt(info.Size(), 10)
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

func mapError(err error, key string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %s", blobster.ErrNotFound, key)
	}
	return err
}
