// Package mem implements an in-memory blobster Bucket.
//
// The driver has real conditional-write and conditional-delete semantics under
// an in-process lock, so it is useful for tests and ephemeral local use without
// being a storage mock.
package mem
