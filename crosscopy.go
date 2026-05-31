package blobster

import (
	"context"
	"sync"
)

// CrossRegionCopier is an optional capability implemented by drivers whose
// backend can copy an object server-side to a destination that may live in a
// different region, bucket, or account, without routing the bytes through the
// caller.
//
// It differs from the base Bucket.Copy in two ways that the cross-region case
// demands. First, the source is a separate Bucket, so the copy can name a source
// in another region/bucket/account rather than a key within the same bucket.
// Second, it returns an asynchronous handle instead of blocking: a cross-region
// copy can take from seconds to many minutes (and some backends run it
// asynchronously on their side), so XCopyFrom starts the copy and returns
// immediately, leaving the caller to observe completion through the handle.
//
// mem and file deliberately do not implement it — there is no region to cross,
// and synthesizing an asynchronous server-side transfer would only add a code
// path the cloud drivers do not exercise. Callers feature-gate with a type
// assertion to CrossRegionCopier or by checking Capabilities.CrossRegionCopy.
type CrossRegionCopier interface {
	// XCopyFrom starts a native server-side copy of srcKey in src into dstKey on
	// the receiver and returns immediately with a handle whose Done channel
	// closes once the copy succeeds, fails, or is cancelled.
	//
	// A non-nil error reports a synchronous setup failure — for example, src is a
	// different backend than the receiver (ErrUnsupported) — and yields no
	// handle. The copy's own outcome is reported through the handle's Err, never
	// through this return.
	//
	// ctx governs the lifetime of the whole copy, not just the XCopyFrom call.
	// Cancelling it requests best-effort cancellation of the in-flight operation;
	// because the transfer runs server-side, the backend may already have
	// finished, so the destination may or may not exist after a cancel. The
	// handle lives only in memory: if the process exits while a copy is in
	// flight, its outcome cannot be recovered.
	XCopyFrom(ctx context.Context, dstKey string, src Bucket, srcKey string, opts *CopyOptions) (*CopyOperation, error)
}

// CopyOperation is a handle to an in-flight or completed cross-region copy
// started by CrossRegionCopier.XCopyFrom. Drivers create it via
// StartCopyOperation; callers consume it via Done and Err.
type CopyOperation struct {
	done chan struct{}
	mu   sync.Mutex
	err  error
}

// Done returns a channel that is closed once the copy reaches a terminal state —
// success, failure, or cancellation. After it is closed, Err reports the
// outcome.
func (o *CopyOperation) Done() <-chan struct{} {
	return o.done
}

// Err returns nil while the copy is still running. Once Done is closed it
// returns nil on success, or the terminal error on failure or cancellation.
func (o *CopyOperation) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

// StartCopyOperation runs fn in a new goroutine under ctx and returns a
// CopyOperation that completes when fn returns. Drivers implementing
// CrossRegionCopier use it so the asynchronous handle — including its
// context-driven cancellation contract — is produced identically across backends
// rather than reimplemented per driver.
func StartCopyOperation(ctx context.Context, fn func(context.Context) error) *CopyOperation {
	op := &CopyOperation{done: make(chan struct{})}
	go func() {
		defer close(op.done)
		err := fn(ctx)
		op.mu.Lock()
		op.err = err
		op.mu.Unlock()
	}()
	return op
}
