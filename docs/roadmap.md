# blobster — Roadmap (planning backlog)

This is the **backlog**: high-level direction and the space of features we
*might* build. It is deliberately loose and cheap to revise — nothing here is a
commitment or a schedule. When an item is concrete enough to build, it graduates
into a **GitHub issue** with its own scope, dependencies, and acceptance
criteria, and status is tracked there. Design decisions live in
[`architecture.md`](architecture.md); see [`../AGENTS.md`](../AGENTS.md) for how
the backlog, issues, and design doc relate.

## Direction

The v0.1 initial release shape is: the base `Bucket` interface, all five drivers
(`mem`, `file`, `s3`, `gcs`, `azureblob`), and three blob-backed utilities built
on the conditional-write primitive: distributed lock, cross-region copy, and a
work queue. The remaining headline utility is multipart parallel upload. The
backlog below tracks that planned helper and additive hardening around the
shipped surface.

## Themes

### Foundation
- The root package: `Bucket` interface, `Attributes`, `Precondition`/conditions,
  sentinel errors, `Capabilities` descriptor, `Locker`, `Queue`,
  `CrossRegionCopier`, and `CopyOperation`.
- `mem` driver — in-memory reference implementation with full conditional
  semantics; the substrate for unit tests.
- `file` driver — filesystem-backed, atomic writes and conditional semantics via
  exclusive create + rename; for local integration and small deployments.

### Cloud drivers
- `s3` driver wrapping a caller-owned client (conditional writes via
  `If-None-Match`/`If-Match`, native multipart, server-side copy, signed URLs).
- `gcs` driver (generation preconditions, resumable upload/compose, rewrite,
  signed URLs).
- `azureblob` driver (ETag preconditions, block upload, async Copy Blob, SAS URLs).
- A shared conformance test suite every driver must pass, run against `mem`/
  `file` by default and real backends behind a build tag.

### Distributed lock
- Lease lock over the conditional-write primitive: acquire/try-acquire/renew/
  release, takeover on lease expiry. Built and shipped in the root package
  (`blobster.NewLocker(bucket, …)`, over any driver that advertises
  `ConditionalWrites`). Fencing tokens were considered and deliberately dropped —
  the lock coordinates multi-object/external critical sections and documents an
  honest best-effort-under-pause contract rather than implying exactly-once (see
  `architecture.md`). A caller builds a native client once and passes the same
  driver for both blob ops and locking.

### Multipart parallel upload
- Define a `MultipartUploader` optional capability and a generic helper over it:
  split, upload parts with bounded concurrency, complete, and abort/clean up on
  failure or cancellation. This public interface/package does not exist yet.
- Tunable part size and concurrency; sensible per-backend defaults.

### Cross-region copy
- `CrossRegionCopier` capability on each cloud driver, returning a uniform,
  async `CopyOperation` handle (`Done()`/`Err()`, created via the root
  `StartCopyOperation`). The capability lives on the driver and the handle in the
  root package — there is no separate `xcopy` package, and `mem`/`file` do not
  implement it.
  - **Done:** S3 (`CopyObject` plus multipart `UploadPartCopy` above the
    single-copy size limit, with abort-on-failure cleanup), GCS (`rewrite`,
    whose token loop the GCS client drives internally for any object size), and
    Azure (async `Copy Blob` polled to completion, sharing one poll helper with
    the base `Copy`).
  - **Resolved — Azure source SAS:** a cross-account Azure source needs a
    short-lived read SAS minted from the *source's* credential (never logged).
    Rather than a new option, the driver mints it from the source `Bucket`'s own
    client — which `XCopyFrom` already receives — via the same `GetSASURL` path
    `SignedURL` uses, so minting a copy SAS and signing a URL are one capability
    with one requirement (a Shared Key credential on the source client).
    Same-account copies (compared by client URL host) carry no SAS; the expiry
    defaults to one hour and is tunable with `azureblob.WithCrossAccountSASExpiry`.
  - **Future — sign from an Entra ID / token credential:** `GetSASURL` needs a
    Shared Key credential, so a token-credential (or SAS-only) source client can
    sign neither a URL nor a cross-account copy SAS today. Closing this needs the
    *user-delegation key* flow (`GetUserDelegationCredential` + the "Storage Blob
    Delegator" role); it is a cross-cutting enhancement that would light up both
    `SignedURL` and cross-account `XCopyFrom` at once, not a
    cross-region-copy-specific one.
  - **Future opt-in — persistent / recoverable handle:** the `CopyOperation`
    handle is in-memory only by default, so a process crash loses an in-flight
    copy's outcome. A planned (not ruled-out) option is a **driver-construction
    choice** (e.g. `WithPersistentCopyHandle`) that persists an operation record
    plus each backend's resume token (S3 multipart upload-id, GCS rewrite token,
    Azure copy-id) under `.blobster/xcopy/`, so another process can re-attach,
    recover, or re-poll. The API is shaped to keep this additive — same
    `XCopyFrom`/`CopyOperation`, backed by bucket state instead of memory. Most
    worthwhile for the genuinely resumable backends; the in-memory default stays
    for the simple/synchronous cases.

### Blob-backed work queue
- Work queue over the conditional-write primitive: competing consumers,
  at-least-once delivery, approximate FIFO. **Built and shipped** in the root
  package (`blobster.NewQueue(bucket, prefix, …)`), alongside the lock — it
  depends only on the root contract and carries no cloud-specific logic. Two
  objects per message — an immutable streamed
  payload and a separate empty-bodied lease record managed exactly like the
  lock's lease — so the heartbeat never re-uploads the payload. Enqueue / Receive
  (poll with backoff) / TryReceive / Ack / Nack, time-sortable ids (epoch-
  millisecond timestamp + UUID), and a
  never-reset `receives` count surfaced on each message. Handlers must be
  idempotent (the lock's liveness-not-safety contract, per message).
  - **Dead-letter / max-receives — built and shipped.** `WithMaxReceives(n)`
    moves a message aside to the queue's `dead/` sub-prefix once it has been
    delivered `n` times, rather than redelivering forever. Lazy and single-mover
    (the claiming worker performs the move after winning the lease; no sweeper),
    the dead record retains the payload, user attributes, and final receive count.
    Unset, dead-lettering is disabled and the never-reset `receives` count still
    lets a caller build its own policy. **Follow-up — `WithDeadLetterQueue`:**
    redirect dead letters into a separate queue instead of the in-prefix `dead/`;
    deferred and additive.
  - **Dedup-keyed enqueue — built and shipped.** `EnqueueWithID` writes a
    message under a caller-supplied id, create-only, reporting `existed=true`
    as a no-op instead of double-delivering — the idempotent-producer primitive
    and the replication watcher's dedup key. The acked-and-gone limitation
    (an acked id is free to re-create) is documented, not tombstoned.
  - **Retention / TTL trim — built and shipped.** `WithRetention(d)` plus an
    explicit, bounded, caller-scheduled `Trim` pass range-deletes messages (and
    their lease records) older than the horizon by id-embedded time — the
    janitor that keeps a broadcast log from growing forever, and the concrete
    form of the old "lifecycle / retention helpers" idea for the queue.
    **Future — watermark-based horizon:** trim only up to the minimum follower
    cursor when a back-channel exists; additive.
  - **Read-only tail — built and shipped.** `Queue.Tail()` iterates the message
    log forward from a caller-persisted cursor without leasing or acking, with
    a configurable cursor lag (`WithTailLag`) that keeps a late-committed
    payload from being skipped. **Future — `StartAfter` listing option:** S3
    and GCS support native start-after listing; plumbing it through
    `ListOptions` would turn the tail's head-scan into a seek; additive.

### Fan-out replication
- **Watcher — built and shipped.** `blobster.NewWatcher(sourceTail, localQueue,
  locker, …)` bridges a read-only source queue (the leader region's broadcast
  log) into a local queue, so existing workers consume replicated messages
  normally: one leader enqueues each change once — leader write cost O(1) in
  the follower count — and each follower's singleton watcher (HA via the lease
  lock) tails the log from a durable, CAS-guarded cursor in its own bucket and
  re-enqueues under the leader's id (idempotent across crash, restart, and
  zombie-holder overlap). Composes the dedup-keyed enqueue, retention trim, and
  read-only tail above; see the "Fan-out replication" section of
  `architecture.md`.
- **Follow-up — reconciliation sweep.** Periodically diff the source log
  against locally-enqueued ids to catch anything the fast path dropped, and let
  a follower that fell past the leader's retention horizon re-bootstrap.

### Cross-cutting
- Signed/presigned URL hardening. `SignedURL` is already a base `Bucket`
  operation gated by `Capabilities().SignedURL`; future work is around signing
  modes such as Azure user-delegation keys and additional conformance coverage.
- Observability seams (metrics/tracing hooks) that keep the root package free of
  any specific telemetry dependency.
- Readiness probe (`Pinger`) for services embedding blobster.

## Exploratory (not yet committed)
- **Resumable multipart** — persist an upload id so an interrupted large upload
  can continue across processes.
- **Content addressing / dedup helpers** — write-by-digest and integrity
  verification on the streamed path.
- **Lifecycle / retention helpers** — TTL and cleanup conventions expressed over
  the keyspace beyond the queue (the queue's own retention shipped as
  `WithRetention`/`Trim`).

## Explicitly out of scope (for now)
- A domain model (packages, datasets, etc.) — that belongs to callers.
- Cross-key transactions beyond single-object conditional writes.
- POSIX-filesystem semantics (partial in-place edits, true moves).
- Ambient cloud configuration or blank-import driver registration — see the
  construction invariant in `architecture.md`.
