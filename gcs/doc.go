// Package gcs implements blobster's Google Cloud Storage driver.
//
// The driver wraps a caller-owned *storage.Client and bucket name. Signed URL
// support is opt-in with WithSignedURLs because GCS signing requires additional
// caller-provided signing material.
package gcs
