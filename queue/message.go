package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/yolocs/blobster"
)

// Message is a claimed work item. Its lease is renewed in the background until
// Ack or Nack is called or the lease is lost. A worker that drops a Message
// without acking or nacking leaks the background renewer and holds the message
// until the process exits, so always settle it with Ack or Nack.
//
// The renewer touches only the empty-bodied lease record, never the payload, so a
// large payload is processed without the heartbeat re-uploading it. Done and Err
// surface a lost lease exactly like the lock's Lock: the loss signal is liveness,
// not a safety guarantee, so handlers must be idempotent.
type Message struct {
	queue     *Queue
	id        string
	msgPath   string
	leasePath string
	owner     string
	attrs     map[string]string
	receives  int

	cancelRenew context.CancelFunc
	stopped     chan struct{} // closed when the renewer goroutine has exited

	mu         sync.Mutex
	version    string
	leaseUntil time.Time
	done       chan struct{}
	err        error
	finished   bool
	settled    bool
}

func (q *Queue) startMessage(id, msgPath, leasePath, owner string, attrs map[string]string, receives int, version string, leaseUntil time.Time) *Message {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Message{
		queue:       q,
		id:          id,
		msgPath:     msgPath,
		leasePath:   leasePath,
		owner:       owner,
		attrs:       blobster.CloneMetadata(attrs),
		receives:    receives,
		cancelRenew: cancel,
		stopped:     make(chan struct{}),
		version:     version,
		leaseUntil:  leaseUntil,
		done:        make(chan struct{}),
	}
	go m.renewLoop(ctx)
	return m
}

// ID returns the message's queue-assigned identifier.
func (m *Message) ID() string { return m.id }

// Receives returns how many times this message has been delivered, counting the
// current delivery. It is 1 on first delivery and increments on each redelivery
// (lease takeover) or Nack-and-reclaim; it is never reset, so a poison message
// that keeps failing reports a growing count rather than looping at 1.
func (m *Message) Receives() int { return m.receives }

// Attributes returns a copy of the user metadata stored with the payload at
// enqueue.
func (m *Message) Attributes() map[string]string { return blobster.CloneMetadata(m.attrs) }

// Body opens a streaming reader over the message payload. The payload is
// immutable, so it can be read even after the lease is lost; the caller is
// responsible for closing the reader.
func (m *Message) Body(ctx context.Context) (blobster.Reader, error) {
	return m.queue.bucket.NewReader(ctx, m.msgPath, nil)
}

// ReadAll reads the entire payload into memory. Prefer Body for large payloads.
func (m *Message) ReadAll(ctx context.Context) ([]byte, error) {
	return m.queue.bucket.ReadAll(ctx, m.msgPath)
}

// Done returns a channel closed when the message is no longer held — settled by
// Ack/Nack or lost (lease could not be renewed). After it closes, Err reports
// which.
func (m *Message) Done() <-chan struct{} { return m.done }

// Err returns nil while the message is held or after a clean Ack/Nack, and
// ErrMessageLost if the lease was lost. It is meaningful once Done is closed.
func (m *Message) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

// Ack acknowledges successful processing and removes the message. It stops the
// renewer, waits for it to exit, then deletes the lease (only if still ours) and
// then the payload. Deleting the lease first means a crash between the two
// deletes costs at most one extra, idempotent redelivery and never leaves an
// orphaned lease that discovery would not revisit. Ack is idempotent and a safe
// no-op once the message was lost or already settled.
func (m *Message) Ack(ctx context.Context) error {
	if !m.settle() {
		return nil
	}
	m.cancelRenew()
	<-m.stopped
	m.finish(nil)

	// The renewer's final CAS may have committed a newer version even if its
	// context was canceled mid-flight, so trust the backend rather than our cached
	// version. Only delete a record that is still ours.
	fields, version, err := m.queue.getRecord(ctx, m.leasePath)
	if errors.Is(err, blobster.ErrNotFound) {
		return nil // lease already gone; nothing of ours to delete
	}
	if err != nil {
		return err
	}
	if fields[leaseOwnerField] != m.owner {
		return nil // taken over by another worker
	}
	if err := m.queue.bucket.Delete(ctx, m.leasePath, blobster.IfMatch(version)); err != nil {
		if errors.Is(err, blobster.ErrPreconditionFailed) || errors.Is(err, blobster.ErrNotFound) {
			return nil // a takeover slipped in between read and delete; leave the payload
		}
		return err
	}
	if err := m.queue.bucket.Delete(ctx, m.msgPath); err != nil && !errors.Is(err, blobster.ErrNotFound) {
		return err
	}
	return nil
}

// Nack returns the message to the queue for immediate redelivery without removing
// it. It stops the renewer and expires the lease in place (deadline = now),
// preserving the receives count so the redelivery increments rather than resets
// it. Nack is idempotent and a safe no-op once the message was lost or already
// settled.
func (m *Message) Nack(ctx context.Context) error {
	if !m.settle() {
		return nil
	}
	m.cancelRenew()
	<-m.stopped
	m.finish(nil)

	fields, version, err := m.queue.getRecord(ctx, m.leasePath)
	if errors.Is(err, blobster.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if fields[leaseOwnerField] != m.owner {
		return nil // taken over; not ours to release
	}
	// Expire the lease (deadline = now) so the next claimer takes over
	// immediately, keeping the existing receives count.
	now := m.queue.clock()
	opts := &blobster.WriterOptions{
		Metadata: map[string]string{
			leaseOwnerField:    m.owner,
			leaseLeaseField:    now.Format(time.RFC3339Nano),
			leaseReceivesField: strconv.Itoa(parseReceives(fields)),
		},
		DisableContentTypeDetection: true,
	}
	if err := m.queue.bucket.WriteAll(ctx, m.leasePath, nil, opts, blobster.IfMatch(version)); err != nil {
		if errors.Is(err, blobster.ErrPreconditionFailed) || errors.Is(err, blobster.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (m *Message) renewLoop(ctx context.Context) {
	defer close(m.stopped)
	ticker := time.NewTicker(m.queue.renew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := m.queue.clock()
		m.mu.Lock()
		leaseUntil := m.leaseUntil
		m.mu.Unlock()
		if !now.Before(leaseUntil) {
			// The lease lapsed before we could renew it; a successor may take over,
			// so stop claiming the message.
			m.finish(ErrMessageLost)
			return
		}

		err := m.renewOnce(ctx, now)
		if errors.Is(err, blobster.ErrPreconditionFailed) || errors.Is(err, blobster.ErrNotFound) {
			m.finish(ErrMessageLost)
			return
		}
		// A transient error is not fatal on its own: keep trying on the next tick
		// until it clears or the lease deadline above is crossed.
	}
}

func (m *Message) renewOnce(ctx context.Context, now time.Time) error {
	m.mu.Lock()
	version := m.version
	m.mu.Unlock()

	newVersion, err := m.queue.writeLease(ctx, m.leasePath, m.owner, now, m.receives, blobster.IfMatch(version))
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.version = newVersion
	m.leaseUntil = now.Add(m.queue.lease)
	m.mu.Unlock()
	return nil
}

// settle marks the message settled exactly once, returning false on later calls
// so Ack/Nack are idempotent and mutually exclusive.
func (m *Message) settle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settled {
		return false
	}
	m.settled = true
	return true
}

func (m *Message) finish(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finished {
		return
	}
	m.finished = true
	m.err = err
	close(m.done)
}
