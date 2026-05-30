package blobtest

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/yolocs/blobster"
)

type BucketFactory func(t *testing.T) blobster.Bucket

func TestBucket(t *testing.T, newBucket BucketFactory) {
	t.Helper()

	t.Run("write read attributes exists and delete", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		opts := &blobster.WriterOptions{
			ContentType:     "text/plain",
			ContentLanguage: "en",
			Metadata: map[string]string{
				"Owner": "tests",
			},
		}
		if err := bucket.WriteAll(ctx, "docs/hello.txt", []byte("hello blobster"), opts); err != nil {
			t.Fatalf("WriteAll: %v", err)
		}

		exists, err := bucket.Exists(ctx, "docs/hello.txt")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !exists {
			t.Fatal("Exists = false, want true")
		}

		got, err := bucket.ReadAll(ctx, "docs/hello.txt")
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if diff := cmp.Diff([]byte("hello blobster"), got); diff != "" {
			t.Fatalf("ReadAll mismatch (-want +got):\n%s", diff)
		}

		attrs, err := bucket.Attributes(ctx, "docs/hello.txt")
		if err != nil {
			t.Fatalf("Attributes: %v", err)
		}
		if attrs.Size != int64(len("hello blobster")) {
			t.Fatalf("Attributes.Size = %d, want %d", attrs.Size, len("hello blobster"))
		}
		if attrs.ContentType != "text/plain" {
			t.Fatalf("Attributes.ContentType = %q, want text/plain", attrs.ContentType)
		}
		if attrs.Metadata["owner"] != "tests" {
			t.Fatalf("Attributes.Metadata[owner] = %q, want tests", attrs.Metadata["owner"])
		}
		if attrs.Version == "" {
			t.Fatal("Attributes.Version is empty")
		}

		if err := bucket.Delete(ctx, "docs/hello.txt"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		exists, err = bucket.Exists(ctx, "docs/hello.txt")
		if err != nil {
			t.Fatalf("Exists after delete: %v", err)
		}
		if exists {
			t.Fatal("Exists after delete = true, want false")
		}
		if _, err := bucket.ReadAll(ctx, "docs/hello.txt"); !errors.Is(err, blobster.ErrNotFound) {
			t.Fatalf("ReadAll after delete error = %v, want ErrNotFound", err)
		}
	})

	t.Run("range readers are seekable and expose metadata", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		if err := bucket.WriteAll(ctx, "letters", []byte("abcdefghijklmnopqrstuvwxyz"), &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
			t.Fatalf("WriteAll: %v", err)
		}

		reader, err := bucket.NewRangeReader(ctx, "letters", 5, 10, nil)
		if err != nil {
			t.Fatalf("NewRangeReader: %v", err)
		}
		defer reader.Close()

		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll range: %v", err)
		}
		if diff := cmp.Diff([]byte("fghijklmno"), got); diff != "" {
			t.Fatalf("range mismatch (-want +got):\n%s", diff)
		}
		if reader.Size() != int64(len("abcdefghijklmnopqrstuvwxyz")) {
			t.Fatalf("Reader.Size = %d, want full object size", reader.Size())
		}
		if reader.ContentType() != "text/plain" {
			t.Fatalf("Reader.ContentType = %q, want text/plain", reader.ContentType())
		}

		if _, err := reader.Seek(2, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		buf := make([]byte, 3)
		if _, err := io.ReadFull(reader, buf); err != nil {
			t.Fatalf("ReadFull after seek: %v", err)
		}
		if diff := cmp.Diff([]byte("hij"), buf); diff != "" {
			t.Fatalf("seeked range mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("upload download and write helpers stream through bucket API", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		if err := bucket.Upload(ctx, "from-reader", strings.NewReader("streamed"), &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
			t.Fatalf("Upload: %v", err)
		}

		var out bytes.Buffer
		if err := bucket.Download(ctx, "from-reader", &out, nil); err != nil {
			t.Fatalf("Download: %v", err)
		}
		if out.String() != "streamed" {
			t.Fatalf("Download wrote %q, want streamed", out.String())
		}

		if err := bucket.WriteAll(ctx, "sniffed", []byte("<html></html>"), nil); err != nil {
			t.Fatalf("WriteAll sniffed: %v", err)
		}
		attrs, err := bucket.Attributes(ctx, "sniffed")
		if err != nil {
			t.Fatalf("Attributes sniffed: %v", err)
		}
		if attrs.ContentType != "text/html; charset=utf-8" {
			t.Fatalf("sniffed ContentType = %q, want text/html; charset=utf-8", attrs.ContentType)
		}
	})

	t.Run("content md5 is enforced", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		sum := md5.Sum([]byte("expected"))
		err := bucket.WriteAll(ctx, "checksum", []byte("actual"), &blobster.WriterOptions{
			ContentType: "text/plain",
			ContentMD5:  sum[:],
		})
		if err == nil {
			t.Fatal("WriteAll with mismatched ContentMD5 succeeded, want error")
		}
		exists, existsErr := bucket.Exists(ctx, "checksum")
		if existsErr != nil {
			t.Fatalf("Exists after failed checksum write: %v", existsErr)
		}
		if exists {
			t.Fatal("object exists after failed checksum write")
		}
	})

	t.Run("conditional writes and deletes are atomic", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		if err := bucket.WriteAll(ctx, "cas", []byte("one"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists); err != nil {
			t.Fatalf("create-only WriteAll: %v", err)
		}
		if err := bucket.WriteAll(ctx, "cas", []byte("two"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists); !errors.Is(err, blobster.ErrPreconditionFailed) {
			t.Fatalf("second create-only WriteAll error = %v, want ErrPreconditionFailed", err)
		}

		attrs, err := bucket.Attributes(ctx, "cas")
		if err != nil {
			t.Fatalf("Attributes: %v", err)
		}
		if err := bucket.WriteAll(ctx, "cas", []byte("two"), &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfMatch(attrs.Version)); err != nil {
			t.Fatalf("IfMatch WriteAll: %v", err)
		}
		if err := bucket.Delete(ctx, "cas", blobster.IfMatch(attrs.Version)); !errors.Is(err, blobster.ErrPreconditionFailed) {
			t.Fatalf("stale IfMatch Delete error = %v, want ErrPreconditionFailed", err)
		}

		attrs, err = bucket.Attributes(ctx, "cas")
		if err != nil {
			t.Fatalf("Attributes after update: %v", err)
		}
		if err := bucket.Delete(ctx, "cas", blobster.IfMatch(attrs.Version)); err != nil {
			t.Fatalf("IfMatch Delete: %v", err)
		}
	})

	t.Run("concurrent create-only writes have exactly one winner", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		bodies := [][]byte{
			[]byte("writer-0"),
			[]byte("writer-1"),
		}
		start := make(chan struct{})
		results := make(chan error, len(bodies))
		var wg sync.WaitGroup
		wg.Add(len(bodies))
		for _, body := range bodies {
			body := body
			go func() {
				defer wg.Done()
				<-start
				results <- bucket.WriteAll(ctx, "race-create", body, &blobster.WriterOptions{ContentType: "text/plain"}, blobster.IfNotExists)
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		successes := 0
		preconditionFailures := 0
		for err := range results {
			switch {
			case err == nil:
				successes++
			case errors.Is(err, blobster.ErrPreconditionFailed):
				preconditionFailures++
			default:
				t.Fatalf("concurrent WriteAll error = %v, want nil or ErrPreconditionFailed", err)
			}
		}
		if successes != 1 || preconditionFailures != len(bodies)-1 {
			t.Fatalf("successes=%d preconditionFailures=%d, want 1 success and %d precondition failure", successes, preconditionFailures, len(bodies)-1)
		}

		got, err := bucket.ReadAll(ctx, "race-create")
		if err != nil {
			t.Fatalf("ReadAll race-created object: %v", err)
		}
		if !bytes.Equal(got, bodies[0]) && !bytes.Equal(got, bodies[1]) {
			t.Fatalf("race-created body = %q, want one writer body", string(got))
		}
	})

	t.Run("copy list list page and sub buckets", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		write := func(key, body string) {
			t.Helper()
			if err := bucket.WriteAll(ctx, key, []byte(body), &blobster.WriterOptions{ContentType: "text/plain"}); err != nil {
				t.Fatalf("WriteAll(%q): %v", key, err)
			}
		}
		write("a/one.txt", "one")
		write("a/two.txt", "two")
		write("a/nested/three.txt", "three")
		write("b/skip.txt", "skip")

		if err := bucket.Copy(ctx, "a/copy.txt", "a/one.txt", nil); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		got, err := bucket.ReadAll(ctx, "a/copy.txt")
		if err != nil {
			t.Fatalf("ReadAll copied object: %v", err)
		}
		if string(got) != "one" {
			t.Fatalf("copied object = %q, want one", string(got))
		}

		page, next, err := bucket.ListPage(ctx, blobster.FirstPageToken, 2, &blobster.ListOptions{Prefix: "a/"})
		if err != nil {
			t.Fatalf("ListPage first: %v", err)
		}
		if len(page) != 2 || len(next) == 0 {
			t.Fatalf("ListPage first len=%d next=%q, want len 2 and non-empty next", len(page), string(next))
		}
		page, next, err = bucket.ListPage(ctx, next, 10, &blobster.ListOptions{Prefix: "a/"})
		if err != nil {
			t.Fatalf("ListPage second: %v", err)
		}
		if len(page) == 0 || len(next) != 0 {
			t.Fatalf("ListPage second len=%d next=%q, want remaining objects and empty next", len(page), string(next))
		}

		iter := bucket.List(&blobster.ListOptions{Prefix: "a/", Delimiter: "/"})
		var keys []string
		for {
			obj, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("List iterator: %v", err)
			}
			if obj.IsDir {
				keys = append(keys, obj.Key+"<dir>")
			} else {
				keys = append(keys, obj.Key)
			}
		}
		slices.Sort(keys)
		wantKeys := []string{"a/copy.txt", "a/nested/<dir>", "a/one.txt", "a/two.txt"}
		if diff := cmp.Diff(wantKeys, keys); diff != "" {
			t.Fatalf("List mismatch (-want +got):\n%s", diff)
		}

		sub := bucket.Sub("a/")
		got, err = sub.ReadAll(ctx, "one.txt")
		if err != nil {
			t.Fatalf("Sub ReadAll: %v", err)
		}
		if string(got) != "one" {
			t.Fatalf("Sub ReadAll = %q, want one", string(got))
		}
	})

	t.Run("canceling writer prevents partial commit", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		writeCtx, cancel := context.WithCancel(ctx)
		writer, err := bucket.NewWriter(writeCtx, "partial", &blobster.WriterOptions{ContentType: "text/plain"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if _, err := writer.Write([]byte("partial body")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		cancel()
		if err := writer.Close(); err == nil {
			t.Fatal("Close after context cancel succeeded, want error")
		}

		exists, err := bucket.Exists(ctx, "partial")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if exists {
			t.Fatal("partial object was committed after writer context cancel")
		}
	})

	t.Run("upload read error prevents partial commit", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		err := bucket.Upload(ctx, "read-error", failingReader{}, &blobster.WriterOptions{ContentType: "text/plain"})
		if err == nil {
			t.Fatal("Upload with failing reader succeeded, want error")
		}
		exists, existsErr := bucket.Exists(ctx, "read-error")
		if existsErr != nil {
			t.Fatalf("Exists after failed upload: %v", existsErr)
		}
		if exists {
			t.Fatal("object exists after failed upload")
		}
	})

	t.Run("signed url exposes unsupported consistently", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		bucket := newBucket(t)

		url, err := bucket.SignedURL(ctx, "missing", &blobster.SignedURLOptions{Method: http.MethodGet})
		if bucket.Capabilities().SignedURL {
			if err != nil {
				t.Fatalf("SignedURL on signed-url capable bucket: %v", err)
			}
			if url == "" {
				t.Fatal("SignedURL returned empty URL")
			}
			return
		}
		if !errors.Is(err, blobster.ErrUnsupported) {
			t.Fatalf("SignedURL error = %v, want ErrUnsupported", err)
		}
	})
}

type failingReader struct{}

func (r failingReader) Read(p []byte) (int, error) {
	copy(p, "partial body")
	return len("partial body"), errors.New("forced read failure")
}
