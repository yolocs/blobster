package azureblob

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/yolocs/blobster"
)

// defaultCrossAccountSASExpiry is how long the read SAS minted for a
// cross-account copy source stays valid. It must outlive the server-side copy,
// since Azure reads the source for the copy's whole duration; callers with very
// large cross-account copies can extend it with WithCrossAccountSASExpiry.
const defaultCrossAccountSASExpiry = time.Hour

// defaultCopyPollInterval is how often Copy Blob status is polled to completion.
const defaultCopyPollInterval = 200 * time.Millisecond

// copier performs one server-side Copy Blob of srcURL into dstKey on the
// destination container, blocking until Azure's asynchronous copy reaches a
// terminal state. It is the seam both the base Copy and the cross-region copy
// depend on, so the poll-to-completion loop is exercised without a live Azure
// backend; the production implementation is clientCopier.
type copier interface {
	copyBlob(ctx context.Context, dstKey, srcURL string, opts *blobster.CopyOptions) error
}

// clientCopier drives Azure's Copy Blob through the destination's caller-owned
// container client. Copy Blob is asynchronous, so a copy that does not complete
// on the initial request is polled to completion via the destination blob's
// properties.
type clientCopier struct {
	client   *container.Client
	interval time.Duration
}

func (c clientCopier) copyBlob(ctx context.Context, dstKey, srcURL string, opts *blobster.CopyOptions) error {
	dst := c.client.NewBlobClient(escapeKey(dstKey, false))
	input := &blob.StartCopyFromURLOptions{}
	if opts != nil && opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(i any) bool { return blobster.AssignNative(input, i) }); err != nil {
			return err
		}
	}
	resp, err := dst.StartCopyFromURL(ctx, srcURL, input)
	if err != nil {
		return mapError(err)
	}
	// Same-account copies usually complete on the first response.
	if deref(resp.CopyStatus) == blob.CopyStatusTypeSuccess {
		return nil
	}
	return pollCopyStatus(ctx, dst, deref(resp.CopyID), c.interval)
}

// copyStatusPoller is the subset of *blob.Client the poll loop needs, so the
// poll-to-completion logic is testable without a live backend.
type copyStatusPoller interface {
	GetProperties(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
}

// pollCopyStatus polls the destination's properties until the copy reaches a
// terminal state. A GetProperties failure mid-copy may be transient, so a few
// are tolerated before giving up; it bails immediately if the context is done.
func pollCopyStatus(ctx context.Context, poller copyStatusPoller, copyID string, interval time.Duration) error {
	const maxPollErrors = 3
	pollErrors := 0
	for {
		props, err := poller.GetProperties(ctx, nil)
		if err != nil {
			pollErrors++
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pollErrors >= maxPollErrors {
				return mapError(err)
			}
			if waitErr := sleep(ctx, interval); waitErr != nil {
				return waitErr
			}
			continue
		}
		pollErrors = 0
		switch deref(props.CopyStatus) {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypePending:
			if waitErr := sleep(ctx, interval); waitErr != nil {
				return waitErr
			}
		default:
			return fmt.Errorf("blobster azure: copy %s: status %q", copyID, deref(props.CopyStatus))
		}
	}
}

// crossCopy carries the resolved seams and identifiers for one cross-region
// copy. Keys are already prefix-resolved.
type crossCopy struct {
	copier      copier
	src         *azureBackend
	dstKey      string
	srcKey      string
	sameAccount bool
	opts        *blobster.CopyOptions
}

func (c crossCopy) run(ctx context.Context) error {
	srcURL, err := c.src.sourceURL(ctx, c.srcKey, c.sameAccount)
	if err != nil {
		return err
	}
	return c.copier.copyBlob(ctx, c.dstKey, srcURL, c.opts)
}

// sourceURL returns the URL addressing srcKey on this backend, for use as a Copy
// Blob source. Within one storage account the plain blob URL suffices: Copy Blob
// authorizes the source read against the destination, and a same-account
// destination credential can already read the source. Across accounts it cannot,
// so the URL must carry a short-lived read SAS minted from this (source)
// backend's own credential — the same GetSASURL path SignedURL uses. A client
// without shared-key signing material fails here exactly as it would for a
// signed URL: cross-account copy needs the same signing ability as SignedURL.
// The SAS is a bearer credential and must never be logged.
func (b *azureBackend) sourceURL(ctx context.Context, srcKey string, sameAccount bool) (string, error) {
	src := b.client.NewBlobClient(escapeKey(srcKey, false))
	if sameAccount {
		return src.URL(), nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	expiry := b.sasExpiry
	if expiry <= 0 {
		expiry = defaultCrossAccountSASExpiry
	}
	signed, err := src.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(expiry), nil)
	if err != nil {
		return "", mapError(err)
	}
	return signed, nil
}

// accountHost is the lowercased host of this backend's container client URL. Two
// buckets share a storage account iff their hosts match; a differing host means a
// cross-account copy, which needs a source read SAS. Standard Azure gives every
// account a distinct host (account.blob.core.windows.net), so this is exact for
// real deployments. It cannot distinguish path-style accounts that share one host
// (the Azurite emulator, some custom endpoints); there a genuinely cross-account
// copy would be issued without a SAS and fail loudly at authorization rather than
// mis-target. That is acceptable: cross-account copy between path-style accounts
// is not a target.
func (b *azureBackend) accountHost() string {
	u, err := url.Parse(b.client.URL())
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}
