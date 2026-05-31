package blobster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yolocs/blobster"
)

func TestStartCopyOperationSuccess(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	op := blobster.StartCopyOperation(ctx, func(ctx context.Context) error {
		return nil
	})

	select {
	case <-op.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not complete")
	}
	if err := op.Err(); err != nil {
		t.Fatalf("Err = %v, want nil", err)
	}
}

func TestStartCopyOperationFailure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	sentinel := errors.New("copy boom")
	op := blobster.StartCopyOperation(ctx, func(ctx context.Context) error {
		return sentinel
	})

	<-op.Done()
	if err := op.Err(); !errors.Is(err, sentinel) {
		t.Fatalf("Err = %v, want %v", err, sentinel)
	}
}

func TestStartCopyOperationCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	started := make(chan struct{})
	op := blobster.StartCopyOperation(ctx, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	<-started
	cancel()

	select {
	case <-op.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not observe cancellation")
	}
	if err := op.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", err)
	}
}

func TestCopyOperationErrNilWhileRunning(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	release := make(chan struct{})
	op := blobster.StartCopyOperation(ctx, func(ctx context.Context) error {
		<-release
		return nil
	})

	if err := op.Err(); err != nil {
		t.Fatalf("Err while running = %v, want nil", err)
	}
	close(release)
	<-op.Done()
	if err := op.Err(); err != nil {
		t.Fatalf("Err after success = %v, want nil", err)
	}
}
