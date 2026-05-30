// Package lock implements a distributed lease lock that is generic over a
// minimal conditional-write store. It is the project's mutual-exclusion
// primitive: it serializes a critical section across processes and hosts so two
// instances of the same workflow do not run at once.
//
// The lock relies on exactly one storage capability — an atomic
// compare-and-swap on a single object — captured by the [Backend] interface.
// Any backend that can create-if-absent, swap-if-version-matches, and
// delete-if-version-matches can host the lock; the lease algorithm itself is
// shared. [FromBucket] adapts any [blobster.Bucket] (and therefore any blobster
// driver — mem, file, s3, gcs, azureblob) to a [Backend], so a caller can build
// a native client once and use it for both blob operations and locking. A
// caller with no blobster bucket at all can implement [Backend] directly over a
// native client.
//
// # Guarantees, honestly
//
// This is a lease lock, not a fencing lock. It provides mutual exclusion in the
// common case and is self-healing: a crashed holder's lease expires and the
// next acquirer takes over. It does NOT protect against a holder that pauses
// (GC, VM freeze) past its lease and then resumes — such a zombie can run its
// critical section once more after a successor has started. The lock makes that
// window small (short lease, active renewer) but cannot close it, because a
// frozen process cannot observe its own freeze. Callers whose critical section
// must be safe under arbitrary pauses are responsible for idempotency (keep
// sections short, make external effects idempotent, or guard a protected blob
// with its own conditional write, whose version token already orders writers).
package lock

import (
	"context"
	"errors"

	"github.com/yolocs/blobster"
)

// Backend-contract sentinel errors. A [Backend] implementation must return
// these (matchable with errors.Is) so the lock can distinguish the conditions
// it acts on from genuine I/O failures.
var (
	// ErrNotExist is returned by Get and Delete when the record is absent.
	ErrNotExist = errors.New("blobster/lock: record does not exist")
	// ErrExists is returned by Create when the record already exists.
	ErrExists = errors.New("blobster/lock: record already exists")
	// ErrConflict is returned by Update and Delete when the current version
	// differs from the expected one (someone else wrote in between).
	ErrConflict = errors.New("blobster/lock: version conflict")
)

// Backend is the minimal conditional single-object store the lock is built on.
// All four operations act on one key; the version is an opaque token (an ETag,
// a GCS generation, …) that the lock never interprets beyond passing it back
// for the next compare-and-swap. Implementations map these onto the backend's
// native conditional-write primitive.
//
// Implementations need not be safe for concurrent use by multiple goroutines on
// the same key from one process; the lock serializes its own access. They must,
// however, provide cross-process atomicity for Create/Update/Delete — that is
// the property the whole lock depends on.
type Backend interface {
	// Get returns the record's stored fields and current version. It returns
	// ErrNotExist if the key is absent.
	Get(ctx context.Context, key string) (fields map[string]string, version string, err error)

	// Create stores fields at key only if the key does not yet exist, returning
	// the new version. It returns ErrExists if the key already exists.
	Create(ctx context.Context, key string, fields map[string]string) (version string, err error)

	// Update stores fields at key only if the current version equals expected,
	// returning the new version. It returns ErrConflict if the version differs,
	// or ErrNotExist if the key has since been deleted.
	Update(ctx context.Context, key string, fields map[string]string, expected string) (version string, err error)

	// Delete removes key only if the current version equals expected. It returns
	// ErrConflict if the version differs and ErrNotExist if the key is absent.
	Delete(ctx context.Context, key string, expected string) error
}

// FromBucket adapts a blobster.Bucket to a Backend, storing lock state in the
// object's user metadata so a single Attributes read returns both the state and
// the version token. The bucket must advertise ConditionalWrites; every
// production driver does.
func FromBucket(b blobster.Bucket) Backend {
	return bucketBackend{b: b}
}

type bucketBackend struct {
	b blobster.Bucket
}

func (s bucketBackend) Get(ctx context.Context, key string) (map[string]string, string, error) {
	attrs, err := s.b.Attributes(ctx, key)
	if err != nil {
		if errors.Is(err, blobster.ErrNotFound) {
			return nil, "", ErrNotExist
		}
		return nil, "", err
	}
	return attrs.Metadata, attrs.Version, nil
}

func (s bucketBackend) Create(ctx context.Context, key string, fields map[string]string) (string, error) {
	opts := &blobster.WriterOptions{Metadata: fields, DisableContentTypeDetection: true}
	if err := s.b.WriteAll(ctx, key, nil, opts, blobster.IfNotExists); err != nil {
		if errors.Is(err, blobster.ErrPreconditionFailed) {
			return "", ErrExists
		}
		return "", err
	}
	return s.version(ctx, key)
}

func (s bucketBackend) Update(ctx context.Context, key string, fields map[string]string, expected string) (string, error) {
	opts := &blobster.WriterOptions{Metadata: fields, DisableContentTypeDetection: true}
	if err := s.b.WriteAll(ctx, key, nil, opts, blobster.IfMatch(expected)); err != nil {
		if errors.Is(err, blobster.ErrPreconditionFailed) {
			return "", ErrConflict
		}
		if errors.Is(err, blobster.ErrNotFound) {
			return "", ErrNotExist
		}
		return "", err
	}
	return s.version(ctx, key)
}

func (s bucketBackend) Delete(ctx context.Context, key string, expected string) error {
	if err := s.b.Delete(ctx, key, blobster.IfMatch(expected)); err != nil {
		if errors.Is(err, blobster.ErrPreconditionFailed) {
			return ErrConflict
		}
		if errors.Is(err, blobster.ErrNotFound) {
			return ErrNotExist
		}
		return err
	}
	return nil
}

// version reads back the freshly written version token. blobster's write path
// does not return it, so the adapter pays one extra Attributes read; a native
// Backend can return the new version from the write directly and skip this.
func (s bucketBackend) version(ctx context.Context, key string) (string, error) {
	attrs, err := s.b.Attributes(ctx, key)
	if err != nil {
		return "", err
	}
	return attrs.Version, nil
}
