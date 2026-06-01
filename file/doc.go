// Package file implements a filesystem-backed blobster Bucket.
//
// It stores object bytes and metadata sidecars under a caller-provided
// directory, commits writes atomically under a per-bucket lock, and implements
// the same conditional-write semantics as the cloud drivers.
package file
