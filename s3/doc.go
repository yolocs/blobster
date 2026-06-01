// Package s3 implements blobster's AWS S3 driver.
//
// The driver wraps a caller-owned *s3.Client and bucket name, preserving caller
// control over credentials, endpoints, retries, and S3-compatible backends such
// as Cloudflare R2.
package s3
