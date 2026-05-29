# blobster — Cloud Backend API Research

Reference material: the cloud-provider API facts that establish blobster's
viability. blobster's whole thesis is that a service needs *nothing but* a blob
store to get a distributed lock, multipart parallel upload, and cross-region
copy. That only holds if each target backend actually exposes the primitives
those utilities require. This document records what we verified, with exact API
surfaces (Go SDK), hard limits, and sources, so we don't have to re-derive it.

The **design conclusions** drawn from this research live in
[`architecture.md`](architecture.md); this file is the evidence behind them. It
is reference, not design — when an API fact changes, update it here and reconcile
the conclusion in the architecture doc.

- **Researched:** 2026-05-28, against official provider docs and the current Go
  SDKs (`aws-sdk-go-v2`, `cloud.google.com/go/storage`,
  `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`).
- **Verdict:** all three headline utilities — lock, cross-region copy, multipart
  upload — are viable on S3, GCS, and Azure with blob storage as the only
  dependency. Caveats are recorded per section.

## Summary

| Utility | S3 | GCS | Azure |
|--------|:--:|:---:|:-----:|
| Lock (conditional writes) | ✅ | ✅ (write-rate bound) | ✅ (CAS, not native lease) |
| Cross-region copy | ✅ (within one partition) | ✅ | ✅ (source SAS required) |
| Multipart parallel upload | ✅ native MPU | ✅ via compose | ✅ via block blobs |

---

## 1. Conditional writes (the lock primitive)

The lease + fencing lock is a generic algorithm over one optimistic-concurrency
operation: **create-only** (first acquisition) and **compare-and-swap on a
version token** (takeover after lease expiry, renewal, safe release). All three
backends provide both atomically, server-side, with exactly-one-winner under
concurrent writers, and all three are strongly consistent.

| | Create-only | CAS overwrite / renew | Conditional delete (release) | Version token | Precondition failure |
|---|---|---|---|---|---|
| **S3** | `If-None-Match: *` on PUT | `If-Match: <etag>` on PUT | `If-Match: <etag>` on DELETE | ETag | HTTP 412 |
| **GCS** | `ifGenerationMatch = 0` | `ifGenerationMatch = <generation>` | `ifGenerationMatch` on delete | generation | HTTP 412 |
| **Azure** | `If-None-Match: *` | `If-Match: <etag>` | `If-Match: <etag>` on Delete Blob | ETag | HTTP 412 |

### S3 — timeline matters
S3 conditional writes are recent; a lot of "S3 can't do locks, use DynamoDB"
lore predates them:
- `If-None-Match: *` (create-only) — GA **2024-08-20**.
- `If-Match: <etag>` (compare-and-swap) — GA **~2024-11-25**, all regions, no
  extra charge.
- `If-Match` on `DeleteObject` (conditional delete) — GA **2025-09-16**. This is
  what makes safe release a single atomic op; before it, release required a
  CAS-to-tombstone workaround. blobster does **not** need that workaround.
- Strong read-after-write consistency has been automatic since **2020-12-01**.

### Go SDK surfaces
- **S3** (`service/s3`): `PutObjectInput.IfNoneMatch` (`*string`, set to `"*"`),
  `PutObjectInput.IfMatch`, `DeleteObjectInput.IfMatch`. No dedicated typed
  error — match with `errors.As(err, &apiErr)` where `apiErr smithy.APIError`,
  then test `apiErr.ErrorCode()` for `"PreconditionFailed"` (412) and
  `"ConditionalRequestConflict"` (409); use `*smithyhttp.ResponseError` for raw
  status.
- **GCS** (`cloud.google.com/go/storage`):
  `obj.If(storage.Conditions{DoesNotExist: true})` for create-only,
  `obj.If(storage.Conditions{GenerationMatch: gen})` for CAS (also works on
  `Delete`). Read the token via `obj.Attrs(ctx).Generation`. Failure is
  `*googleapi.Error` with `Code == 412`; absence is `storage.ErrObjectNotExist`.
- **Azure** (`azblob`): `ModifiedAccessConditions{IfMatch, IfNoneMatch}` — typed
  `*azcore.ETag`, not `*string`; use `azcore.ETagAny` for `*`. Failure:
  `bloberror.HasCode(err, bloberror.ConditionNotMet)`; create-only races may also
  surface as `bloberror.BlobAlreadyExists` — match both.

### Fencing token
The fencing token must be **blobster's own monotonic counter inside the lock
record** (the `fence` field), bumped via CAS on every acquisition. The native
version tokens are not usable as fences: S3/Azure ETags are opaque, and GCS
generations are not a clean caller-facing counter.

### Azure native lease vs uniform CAS
Azure has a first-class **Lease Blob** API (acquire/renew/change/release/break,
15–60s or infinite). It is good mutual exclusion but **provides no fencing
token** — the lease ID is a random GUID, not monotonic, and it gates writes to
the leased blob itself, not to an external protected resource. Getting a fence
would mean layering a CAS counter on top anyway. **Decision: build the lock on
uniform ETag CAS across all three clouds.** Native leases stay a possible
Azure-only optimization behind an optional capability, never the core algorithm.

### Gotchas
- **GCS limits writes to ~1 per second per object name.** A single hot lock key
  with sub-second lease renewals *will* be throttled. This bounds the lock's
  defaults: lease/renew intervals on the order of seconds, jittered backoff on
  429/503. (The per-bucket ceiling is not the constraint; the per-object one is.)
- S3 contended-lock writes share one key/prefix (~3,500 PUT/COPY/DELETE per
  second per prefix) — fine for a coarse lock; back off rather than hammer.
- On the S3 CAS path treat 409 `ConditionalRequestConflict` as retryable
  (re-read ETag); an `If-Match` 404 means concurrent deletion — re-evaluate
  acquisition.

---

## 2. Cross-region copy

Each backend has a native server-side copy that moves bytes between regions
without round-tripping through the caller. Confirmed from docs (not inferred):
each provider documents *inter-region bandwidth billing* for the operation,
which only makes sense if the transfer is server-to-server.

| | Mechanism | Model | Size handling |
|---|---|---|---|
| **S3** | `CopyObject` (`x-amz-copy-source`) | **Synchronous** (done on return) | ≤5 GB single call; `UploadPartCopy` (multipart copy) above |
| **GCS** | `rewrite` | **Sync-poll** (`rewriteToken` loop) | Single call when same-location/class; multi-call otherwise |
| **Azure** | `Copy Blob` | **Asynchronous** (copy-id + status poll) | Any size; sync `CopyFromURL` ≤256 MiB, `Put Block From URL` for larger |

The three shapes (sync / sync-poll / async) are why the `xcopy` API returns a
`CopyOperation` handle with `Wait`/`Poll`; the handle must treat "already
complete on return" (S3) as a first-class case.

### Go SDK surfaces
- **S3**: `client.CopyObject(ctx, &s3.CopyObjectInput{Bucket, Key, CopySource})`;
  `UploadPartCopyInput{CopySource, CopySourceRange}` for >5 GB. The SDK detects
  the "HTTP 200 with an embedded XML error" case (which can occur on long
  cross-region copies) and surfaces it as a real error — don't short-circuit the
  response body.
- **GCS**: `dst.CopierFrom(src)` → `(*Copier).Run(ctx)` drives the rewrite-token
  loop transparently; `Copier.RewriteToken` (resume after error),
  `Copier.ProgressFunc(copied, total)`, `Copier.DestinationKMSKeyName`.
- **Azure**: `blob.Client.StartCopyFromURL` (async) → `CopyID`, `CopyStatus`;
  poll `blob.Client.GetProperties` for `CopyStatus`
  (`Pending|Success|Aborted|Failed`); `AbortCopyFromURL`; sync `CopyFromURL`.

### Constraints that bite
- **S3: cross-region only within one AWS partition.** `aws` ↔ `aws-cn` ↔
  `aws-us-gov` cannot be crossed server-side. Also: S3 Transfer Acceleration and
  VPC gateway endpoints break cross-region copy; Multi-Region Access Points are
  valid only as the *destination*. Max object via single `CopyObject` is 5 GB
  (doc now lists max object size as 48.8 TiB via multipart).
- **GCS: the single-shot `copy` op fails on large/cross-region copies**
  (`Payload too large`) — must use the `rewrite` loop (`Copier.Run`). Use the
  **global** endpoint, not a regional one. Rewrite token expires after 1 week.
  CMEK key must be in the destination location.
- **Azure: a cross-account source must carry a read SAS** (Entra ID / Shared Key
  authorize only the destination). Only one pending copy per destination blob
  (else 409). Pending copies time out after two weeks. **Never log the source
  SAS URL.**

---

## 3. Multipart / parallel upload

All three can upload a large object as concurrent parts, but only S3 has a
native upload-id/part/complete/abort model. The proposed `MultipartUploader`
interface is S3-shaped, and **three of its four concepts do not exist natively on
GCS or Azure** — so the interface must be defined in backend-honest terms.

| Interface concept | S3 | GCS | Azure |
|---|---|---|---|
| Mechanism | Native MPU | **Compose** (parallel composite upload) | Block blobs (`StageBlock` + `CommitBlockList`) |
| `uploadID` | Real server-side ID | **None** — synthesize (temp-component key prefix) | **None** — synthesize; block IDs are client-chosen, scoped to the blob key |
| `UploadPart` (concurrent) | `UploadPart` → ETag | Write a temp object per part | `StageBlock` — **returns no per-part ETag** |
| `Complete` | `CompleteMultipartUpload(parts)` | `ComposerFrom(...).Run` — **≤32 sources/call → tree-compose** | `CommitBlockList(orderedIDs)` |
| `Abort` | Real `AbortMultipartUpload` | **list + delete** temp components | **No abort** — uncommitted blocks GC after ~7 days |
| Limits | 5 MiB–5 GiB/part, 10k parts, 48.8 TiB object | ≤32 sources/compose; composite has **no MD5** | 4 GiB/block, 50k blocks, ~190.7 TiB, 100k uncommitted ceiling |

### Why the GCS path is compose, not XML multipart
GCS *does* expose an S3-compatible XML-API multipart upload, which would be the
clean conceptual fit — but it is **absent from `cloud.google.com/go/storage`**
(open issue `googleapis/google-cloud-go#11609`). Using it would mean smuggling in
a second, HMAC-keyed client, violating blobster's "caller-owned native SDK
client" invariant. Compose stays inside the official client, so it is the chosen
mechanism. Resumable upload (`storage.Writer.ChunkSize`) is sequential and is
ruled out for parallelism.

### Go SDK surfaces
- **S3**: low-level `CreateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`. Prefer these
  over the high-level `feature/s3/manager` Uploader (now deprecated in favor of
  `feature/s3/transfermanager`) to keep control of concurrency and the
  caller-owned client. `UploadPart` needs a seekable body or explicit
  `ContentLength` to compute the payload hash.
- **GCS**: `dst.ComposerFrom(srcs...).Run(ctx)`; components are real objects and
  must be cleaned up (place them under the reserved `.blobster/multipart/`
  prefix). Composite objects have no MD5 (CRC32C only).
- **Azure**: `blockblob.Client.StageBlock`, `CommitBlockList`, `GetBlockList`;
  high-level `UploadStream` / `UploadBuffer` / `UploadFile` parallelize
  internally (`BlockSize`, `Concurrency`).

### Design implications for the interface
- `uploadID` and `partETag` must be **blobster-owned opaque tokens**, not literal
  backend handles. S3 can back them with the real upload-id/ETag; GCS/Azure
  synthesize them.
- `Abort` must be specified as **best-effort cleanup** — GCS deletes temp
  components, Azure relies on GC, S3 calls the real abort. Pair with a backend
  lifecycle rule (S3 `AbortIncompleteMultipartUpload`; GCS/Azure TTL on the temp
  namespace) as a crash backstop.
- **Azure has no per-upload isolation on a key**: two concurrent uploads to the
  same key share one uncommitted-block namespace and would corrupt each other.
  Namespace block IDs by the synthesized `uploadID`, and pad them to equal length
  (Azure requires all block IDs in a blob to be the same length).

---

## Sources

**AWS S3**
- Conditional writes (412/409/404 semantics): https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- Conditional writes GA (2024-08): https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
- If-Match / CAS (2024-11): https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/
- Conditional deletes (2025-09): https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets/
- CopyObject (cross-region, 5 GB cap, 200-with-error): https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
- UploadPartCopy: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
- Copy/move (cross-region example): https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html
- Multipart limits: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
- Partitions (cross-partition impossible): https://docs.aws.amazon.com/whitepapers/latest/aws-fault-isolation-boundaries/partitions.html
- Strong consistency (2020-12): https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/
- aws-sdk-go-v2 s3: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3
- aws-sdk-go-v2 manager (deprecated): https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager

**Google Cloud Storage**
- Request preconditions: https://docs.cloud.google.com/storage/docs/request-preconditions
- Consistency: https://docs.cloud.google.com/storage/docs/consistency
- Objects: rewrite: https://docs.cloud.google.com/storage/docs/json_api/v1/objects/rewrite
- Objects: copy: https://docs.cloud.google.com/storage/docs/json_api/v1/objects/copy
- Copy/rename/move: https://docs.cloud.google.com/storage/docs/copying-renaming-moving-objects
- Quotas (1 write/sec/object): https://docs.cloud.google.com/storage/quotas
- Compose objects: https://docs.cloud.google.com/storage/docs/composing-objects
- XML multipart uploads: https://docs.cloud.google.com/storage/docs/multipart-uploads
- Go SDK XML MPU gap (#11609): https://github.com/googleapis/google-cloud-go/issues/11609
- Go SDK: https://pkg.go.dev/cloud.google.com/go/storage

**Azure Blob Storage**
- Conditional headers: https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
- Manage concurrency (ETag): https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage
- Lease Blob: https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob
- Copy Blob (async, SAS, timeout): https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob
- Copy Blob From URL: https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob-from-url
- Put Block / Put Block List: https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
- Go SDK blob: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob
- Go SDK blockblob: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob
- Go SDK bloberror: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror
