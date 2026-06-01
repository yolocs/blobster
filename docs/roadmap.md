# blobster — Roadmap (planning backlog)

This is the **backlog**: high-level direction and the space of features we
*might* build. It is deliberately loose and cheap to revise — nothing here is a
commitment or a schedule. When an item is concrete enough to build, it graduates
into a **GitHub issue** with its own scope, dependencies, and acceptance
criteria, and status is tracked there. Design decisions live in
[`architecture.md`](architecture.md); see [`../AGENTS.md`](../AGENTS.md) for how
the backlog, issues, and design doc relate.

## Direction

The first useful shape of blobster is: the base `Bucket` interface, all five
drivers (`mem`, `file`, `s3`, `gcs`, `azure`), and the three headline utilities
(distributed lock, multipart parallel upload, cross-region copy) built on the
conditional-write primitive. The `mem`/`file` drivers come first because they
pin the interface and are the test substrate everything else relies on.

## Themes

### Foundation
- The root package: `Bucket` interface, `Attributes`, `Precondition`/conditions,
  sentinel errors, `Capabilities` descriptor, and the optional-capability
  interface definitions.
- `mem` driver — in-memory reference implementation with full conditional
  semantics; the substrate for unit tests.
- `file` driver — filesystem-backed, atomic writes and conditional semantics via
  exclusive create + rename; for local integration and small deployments.

### Cloud drivers
- `s3` driver wrapping a caller-owned client (conditional writes via
  `If-None-Match`/`If-Match`, native multipart, server-side copy, signed URLs).
- `gcs` driver (generation preconditions, resumable upload/compose, rewrite,
  signed URLs).
- `azure` driver (ETag preconditions, block upload, async Copy Blob, SAS URLs).
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
- Generic helper over `MultipartUploader`: split, upload parts with bounded
  concurrency, complete, and abort/clean up on failure or cancellation.
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
    `SignedURL` and cross-account `XCopyFrom` at once, not an xcopy-specific one.
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

### Cross-cutting
- Signed/presigned URLs as a first-class capability (`SignedURLer`).
- Observability seams (metrics/tracing hooks) that keep the root package free of
  any specific telemetry dependency.
- Readiness probe (`Pinger`) for services embedding blobster.

## Exploratory (not yet committed)
- **Resumable multipart** — persist an upload id so an interrupted large upload
  can continue across processes.
- **Content addressing / dedup helpers** — write-by-digest and integrity
  verification on the streamed path.
- **Lifecycle / retention helpers** — TTL and cleanup conventions expressed over
  the keyspace.

## Explicitly out of scope (for now)
- A domain model (packages, datasets, etc.) — that belongs to callers.
- Cross-key transactions beyond single-object conditional writes.
- POSIX-filesystem semantics (partial in-place edits, true moves).
- Ambient cloud configuration or blank-import driver registration — see the
  construction invariant in `architecture.md`.
