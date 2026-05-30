package blobster

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var (
	ErrNotFound           = errors.New("blobster: not found")
	ErrPreconditionFailed = errors.New("blobster: precondition failed")
	ErrUnsupported        = errors.New("blobster: unsupported")
	ErrInvalidOption      = errors.New("blobster: invalid option")
)

var FirstPageToken = []byte("first page")

const DefaultSignedURLExpiry = time.Hour

type Bucket interface {
	As(i any) bool
	Attributes(ctx context.Context, key string) (*Attributes, error)
	Close() error
	Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions, preconditions ...Precondition) error
	Delete(ctx context.Context, key string, preconditions ...Precondition) error
	Download(ctx context.Context, key string, w io.Writer, opts *ReaderOptions) error
	ErrorAs(err error, i any) bool
	Exists(ctx context.Context, key string) (bool, error)
	IsAccessible(ctx context.Context) (bool, error)
	List(opts *ListOptions) *ListIterator
	ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *ListOptions) ([]*ListObject, []byte, error)
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (Reader, error)
	NewReader(ctx context.Context, key string, opts *ReaderOptions) (Reader, error)
	NewWriter(ctx context.Context, key string, opts *WriterOptions, preconditions ...Precondition) (Writer, error)
	ReadAll(ctx context.Context, key string) ([]byte, error)
	SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error)
	Sub(prefix string) Bucket
	Upload(ctx context.Context, key string, r io.Reader, opts *WriterOptions, preconditions ...Precondition) error
	WriteAll(ctx context.Context, key string, p []byte, opts *WriterOptions, preconditions ...Precondition) error
	Capabilities() Capabilities
}

type Reader interface {
	io.ReadSeekCloser
	io.WriterTo
	As(i any) bool
	ContentType() string
	ModTime() time.Time
	Size() int64
}

type Writer interface {
	io.WriteCloser
	io.ReaderFrom
	CloseWithError(error) error
}

type Attributes struct {
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	ContentType        string
	Metadata           map[string]string
	CreateTime         time.Time
	ModTime            time.Time
	Size               int64
	MD5                []byte
	Version            string
	Native             any
}

func (a *Attributes) As(i any) bool {
	if a == nil {
		return false
	}
	return AssignNative(a.Native, i)
}

func (a *Attributes) Clone() *Attributes {
	if a == nil {
		return nil
	}
	out := *a
	out.Metadata = CloneMetadata(a.Metadata)
	out.MD5 = bytes.Clone(a.MD5)
	return &out
}

type Capabilities struct {
	ConditionalWrites bool
	Copy              bool
	List              bool
	ListPage          bool
	RangeRead         bool
	SignedURL         bool
}

type CopyOptions struct {
	BeforeCopy func(asFunc func(any) bool) error
}

type ReaderOptions struct {
	BeforeRead func(asFunc func(any) bool) error
}

type WriterOptions struct {
	BufferSize                  int
	MaxConcurrency              int
	CacheControl                string
	ContentDisposition          string
	ContentEncoding             string
	ContentLanguage             string
	ContentType                 string
	DisableContentTypeDetection bool
	ContentMD5                  []byte
	Metadata                    map[string]string
	BeforeWrite                 func(asFunc func(any) bool) error
}

func (o *WriterOptions) Clone() (*WriterOptions, error) {
	if o == nil {
		return &WriterOptions{}, nil
	}
	out := *o
	metadata, err := NormalizeMetadata(o.Metadata)
	if err != nil {
		return nil, err
	}
	out.Metadata = metadata
	out.ContentMD5 = bytes.Clone(o.ContentMD5)
	return &out, nil
}

type ListOptions struct {
	Prefix     string
	Delimiter  string
	BeforeList func(asFunc func(any) bool) error
}

type ListObject struct {
	Key     string
	ModTime time.Time
	Size    int64
	MD5     []byte
	IsDir   bool
	Native  any
}

func (o *ListObject) As(i any) bool {
	if o == nil {
		return false
	}
	return AssignNative(o.Native, i)
}

type SignedURLOptions struct {
	Expiry                   time.Duration
	Method                   string
	ContentType              string
	EnforceAbsentContentType bool
	BeforeSign               func(asFunc func(any) bool) error
}

type ListIterator struct {
	bucket    Bucket
	opts      *ListOptions
	pageToken []byte
	page      []*ListObject
	done      bool
	pageSize  int
}

func NewListIterator(bucket Bucket, opts *ListOptions) *ListIterator {
	return &ListIterator{
		bucket:    bucket,
		opts:      cloneListOptions(opts),
		pageToken: bytes.Clone(FirstPageToken),
		pageSize:  1000,
	}
}

func (i *ListIterator) Next(ctx context.Context) (*ListObject, error) {
	for {
		if len(i.page) > 0 {
			next := i.page[0]
			i.page = i.page[1:]
			return next, nil
		}
		if i.done {
			return nil, io.EOF
		}

		page, next, err := i.bucket.ListPage(ctx, i.pageToken, i.pageSize, i.opts)
		if err != nil {
			return nil, err
		}
		i.pageToken = bytes.Clone(next)
		if len(next) == 0 {
			i.done = true
		}
		i.page = page
	}
}

func (i *ListIterator) All(ctx context.Context) (iter.Seq2[*ListObject, func(io.Writer, *ReaderOptions) error], func() error) {
	var iteratorErr error
	seq := func(yield func(*ListObject, func(io.Writer, *ReaderOptions) error) bool) {
		for {
			obj, err := i.Next(ctx)
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				iteratorErr = err
				return
			}
			download := func(w io.Writer, opts *ReaderOptions) error {
				return i.bucket.Download(ctx, obj.Key, w, opts)
			}
			if !yield(obj, download) {
				return
			}
		}
	}
	return seq, func() error { return iteratorErr }
}

type Preconditions struct {
	IfNotExists bool
	IfMatch     string
	IfNotMatch  string
}

type Precondition interface {
	apply(*Preconditions)
}

type ifNotExistsPrecondition struct{}

func (ifNotExistsPrecondition) apply(p *Preconditions) {
	p.IfNotExists = true
}

var IfNotExists Precondition = ifNotExistsPrecondition{}

type ifMatchPrecondition string

func (p ifMatchPrecondition) apply(out *Preconditions) {
	out.IfMatch = string(p)
}

func IfMatch(version string) Precondition {
	return ifMatchPrecondition(version)
}

type ifNotMatchPrecondition string

func (p ifNotMatchPrecondition) apply(out *Preconditions) {
	out.IfNotMatch = string(p)
}

func IfNotMatch(version string) Precondition {
	return ifNotMatchPrecondition(version)
}

func CompilePreconditions(preconditions []Precondition) (Preconditions, error) {
	var out Preconditions
	for _, p := range preconditions {
		if p == nil {
			continue
		}
		p.apply(&out)
	}
	n := 0
	if out.IfNotExists {
		n++
	}
	if out.IfMatch != "" {
		n++
	}
	if out.IfNotMatch != "" {
		n++
	}
	if n > 1 {
		return Preconditions{}, fmt.Errorf("%w: multiple mutually exclusive preconditions", ErrInvalidOption)
	}
	return out, nil
}

func AssignNative(native any, i any) bool {
	if native == nil || i == nil {
		return false
	}
	dst := reflect.ValueOf(i)
	if dst.Kind() != reflect.Pointer || dst.IsNil() {
		return false
	}
	value := reflect.ValueOf(native)
	if !value.Type().AssignableTo(dst.Elem().Type()) {
		return false
	}
	dst.Elem().Set(value)
	return true
}

func CloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func NormalizeMetadata(metadata map[string]string) (map[string]string, error) {
	if metadata == nil {
		return nil, nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if key == "" {
			return nil, fmt.Errorf("%w: metadata key cannot be empty", ErrInvalidOption)
		}
		normalized := strings.ToLower(key)
		if _, exists := out[normalized]; exists {
			return nil, fmt.Errorf("%w: duplicate metadata key %q", ErrInvalidOption, key)
		}
		out[normalized] = value
	}
	return out, nil
}

func PrepareWriteAllOptions(opts *WriterOptions, p []byte) (*WriterOptions, error) {
	out, err := opts.Clone()
	if err != nil {
		return nil, err
	}
	if out.ContentType == "" && !out.DisableContentTypeDetection {
		out.ContentType = http.DetectContentType(p)
	}
	if len(out.ContentMD5) == 0 {
		sum := md5.Sum(p)
		out.ContentMD5 = sum[:]
	}
	return out, nil
}

func RequireUploadOptions(opts *WriterOptions) (*WriterOptions, error) {
	out, err := opts.Clone()
	if err != nil {
		return nil, err
	}
	if out.ContentType == "" {
		return nil, fmt.Errorf("%w: Upload requires WriterOptions.ContentType", ErrInvalidOption)
	}
	return out, nil
}

func cloneListOptions(opts *ListOptions) *ListOptions {
	if opts == nil {
		return &ListOptions{}
	}
	out := *opts
	return &out
}
