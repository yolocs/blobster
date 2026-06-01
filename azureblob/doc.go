// Package azureblob implements blobster's Azure Blob Storage driver.
//
// The driver wraps a caller-owned *container.Client, where the container is the
// blobster bucket boundary. It preserves caller control over credentials and
// endpoints, including the shared-key credentials needed for SAS URLs.
package azureblob
