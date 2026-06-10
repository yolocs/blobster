// Package blobster defines a cloud-agnostic blob-storage interface and
// coordination utilities built on conditional writes.
//
// A Bucket models a flat keyspace rooted at a prefix. Implementations stream
// reads and writes, expose optional capabilities explicitly, and are
// constructed from caller-owned cloud clients rather than ambient configuration
// or driver registration side effects.
//
// The root package also provides a lease Locker, a blob-backed Queue (with a
// read-only Tail view and retention Trim), a fan-out replication Watcher, and
// the CrossRegionCopier handle types. Concrete drivers live in subpackages such
// as mem, file, s3, gcs, and azureblob.
package blobster
