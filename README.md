# blobster

<img width="2912" height="1440" alt="Gemini_Generated_Image_owsi9kowsi9kowsi" src="https://github.com/user-attachments/assets/feed8fd6-98e5-4851-9136-bc75ba1e006b" />


Cloud-agnostic storage and coordination primitives built solely on blob storage.

blobster is for teams that already have object storage everywhere and want to
keep the rest of the dependency graph small. If you run across multiple clouds,
or you are forced by platform, customer, or compliance boundaries to minimize
extra infrastructure, a bucket is often the one primitive you can count on.

The project turns that bucket into a practical foundation: ordinary blob
operations, conditional writes, a lease lock, a blob-backed work queue, and
native server-side copy between regions/buckets/accounts where the backend
supports it. There is no Redis, database, broker, sidecar, driver registry, or
ambient cloud configuration. Callers pass in native SDK clients they configured
themselves.

The aim is not to replace every specialized service. It is to provide a small,
boring substrate that is good enough for services that value portability,
explicit construction, and minimum dependencies.

## Status (v0.1 initial release candidate)

Implemented:

- root `blobster.Bucket` API for attributes, exists, read, range-read, write,
  upload/download helpers, delete, copy, list/list-page, sub-buckets,
  capabilities, preconditions, and signed URL hooks
- `mem` driver with real conditional-write semantics
- `file` driver with atomic writes and conditional semantics (write-temp +
  rename under a per-bucket lock)
- `gcs` driver built from a caller-owned `*storage.Client`
- `s3` driver built from a caller-owned `*s3.Client` (conditional
  writes, streaming multipart uploads, server-side copy, presigned URLs); also
  serves S3-compatible backends like Cloudflare R2 by pointing the client at
  their endpoint — no separate driver
- `azureblob` driver built from a caller-owned `*container.Client` (conditional
  writes, streaming block-blob uploads, async server-side copy, SAS URLs)
- shared conformance tests plus GCS, S3, and Azure cloud tests behind the
  `cloud` build tag
- lease-based distributed lock (`blobster.NewLocker`) over the conditional-write
  primitive
- cross-region copy as an optional driver capability (`s3`, `gcs`, `azureblob`)
- blob-backed work queue (`blobster.NewQueue`) — competing consumers,
  at-least-once delivery, approximate FIFO, built on the conditional-write
  primitive; with dedup-keyed enqueue (`EnqueueWithID`), retention trim
  (`WithRetention`/`Trim`), and a read-only tail view (`Queue.Tail`)
- fan-out replication watcher (`blobster.NewWatcher`) — bridges a read-only
  source queue (a leader's broadcast log) into a local queue, singleton per
  region via the lease lock, resuming from a durable cursor

Planned:

- generic multipart parallel-upload helper over a future `MultipartUploader`
  capability

## Usage

Construct a bucket from a caller-owned native SDK client:

```go
ctx := context.Background()
client, err := storage.NewClient(ctx)
if err != nil {
	return err
}
defer client.Close()

bucket := gcs.New(client, "my-bucket", gcs.WithPrefix("app/"))
```

The same pattern applies to the other drivers: `s3.New(client, bucket, ...)`,
`azureblob.New(containerClient, ...)`, `file.New(dir)`, and `mem.New()`.

### Basic blobs

The base `Bucket` API covers read, write, range-read, list, copy, delete,
attributes, metadata updates, preconditions, and signed URL hooks.

```go
if err := bucket.WriteAll(ctx, "users/42.json", []byte(`{"name":"Ada"}`), &blobster.WriterOptions{
	ContentType: "application/json",
	Metadata:    map[string]string{"owner": "accounts"},
}, blobster.IfNotExists); err != nil {
	return err
}

attrs, err := bucket.Attributes(ctx, "users/42.json")
if err != nil {
	return err
}

_, err = bucket.UpdateMetadata(ctx, "users/42.json", map[string]string{
	"owner": "accounts",
	"state": "indexed",
})
if err != nil {
	return err
}

body, err := bucket.ReadAll(ctx, "users/42.json")
if err != nil {
	return err
}
_ = body

// Optimistic update: only replace the object if the version we read is current.
err = bucket.WriteAll(ctx, "users/42.json", []byte(`{"name":"Ada Lovelace"}`), &blobster.WriterOptions{
	ContentType: "application/json",
}, blobster.IfMatch(attrs.Version))
if err != nil {
	return err
}
```

### Distributed lock

`blobster.NewLocker` builds a lease lock on top of conditional writes. It is a
portable coordination primitive for short critical sections; like any lease
lock, handlers should be idempotent because a paused process can outlive its
lease.

```go
locker := blobster.NewLocker(bucket)

lock, err := locker.Acquire(ctx, "rollups/daily", blobster.WithLockOwner("worker-7"))
if err != nil {
	return err
}
defer func() {
	_ = lock.Release(context.Background())
}()

if err := runDailyRollup(ctx); err != nil {
	return err
}

select {
case <-lock.Done():
	return lock.Err() // ErrLockLost means the lease was no longer held.
default:
	return nil
}
```

### Work queue

`blobster.NewQueue` turns any bucket that supports conditional writes into a
competing-consumers work queue. It owns a caller-supplied prefix and stores each
message as an immutable payload plus a separate lease record; handlers must be
idempotent (delivery is at-least-once). `WithMaxReceives(n)` opts into
dead-lettering: after `n` deliveries a poison message is moved aside to the
queue's `dead/` sub-prefix (payload, user attributes, and final receive count
retained) instead of being redelivered forever.

```go
q, err := blobster.NewQueue(bucket, "jobs/",
	blobster.WithQueueVisibilityLease(15*time.Second),
	blobster.WithMaxReceives(5),
)
if err != nil {
	return err
}

if _, err := q.Enqueue(ctx, strings.NewReader("do the thing"), nil); err != nil {
	return err
}

msg, err := q.Receive(ctx) // polls with backoff until a message is claimed
if err != nil {
	return err
}
body, err := msg.ReadAll(ctx)
if err != nil {
	_ = msg.Nack(ctx) // return it for redelivery
	return err
}

if err := handleJob(ctx, body); err != nil {
	_ = msg.Nack(ctx)
	return err
}
return msg.Ack(ctx) // processed; remove it
```

`EnqueueWithID` enqueues create-only under a caller-supplied id, so a retried
producer is an idempotent no-op (`existed=true`) instead of a duplicate.
`WithRetention(d)` plus an explicit `Trim` call range-deletes messages older
than the horizon — the janitor for a queue used as a broadcast log that nobody
acks empty.

### Fan-out replication

A queue can also be *tailed* read-only (`q.Tail()`) — iterated forward from a
cursor without leasing or acking. `blobster.NewWatcher` builds on that to
replicate a leader region's queue into a follower's: the leader enqueues each
message once, and each follower's watcher (a singleton elected via the lease
lock, resuming from a durable cursor in the follower's own bucket) re-enqueues
it locally under the leader's id, so local workers consume it normally and
replays dedup. See the "Fan-out replication" section of
[`docs/architecture.md`](docs/architecture.md).

```go
leaderTail := leaderQueue.Tail() // leaderQueue may be over a read-only bucket
w, err := blobster.NewWatcher(leaderTail, localQueue, blobster.NewLocker(followerBucket),
	blobster.WithWatchName("leader-events"),
)
if err != nil {
	return err
}
return w.Run(ctx) // contend, replicate, and stand by again until ctx is done
```

### Cross-region copy

Cloud drivers that can copy server-side implement `blobster.CrossRegionCopier`.
The source and destination must be buckets from the same backend family (`s3` to
`s3`, `gcs` to `gcs`, `azureblob` to `azureblob`), but they may point at
different buckets, regions, containers, or accounts when the provider supports
that shape. The bytes do not stream through your process.

```go
src := s3.New(srcClient, "source-bucket", s3.WithPrefix("prod/"))
dst := s3.New(dstClient, "archive-bucket", s3.WithPrefix("snapshots/"))

copier, ok := blobster.Bucket(dst).(blobster.CrossRegionCopier)
if !ok {
	return blobster.ErrUnsupported
}

op, err := copier.XCopyFrom(ctx, "users/42.json", src, "users/42.json", nil)
if err != nil {
	return err
}

select {
case <-op.Done():
	return op.Err()
case <-ctx.Done():
	return ctx.Err()
}
```

For tests:

```sh
go test ./...
go test -race ./...
```

Cloud-backed tests:

```sh
BLOBSTER_GCS_BUCKET=my-test-bucket go test -tags cloud ./gcs
```

Cloudflare R2 reuses the `s3` driver against R2's endpoint. The client must set
`RequestChecksumCalculation`/`ResponseChecksumValidation` to *when-required*
(`aws-sdk-go-v2`'s default CRC32 request checksum is rejected by R2):

```sh
BLOBSTER_R2_BUCKET=my-bucket BLOBSTER_R2_ENDPOINT=https://<acct>.r2.cloudflarestorage.com \
  BLOBSTER_R2_ACCESS_KEY_ID=... BLOBSTER_R2_SECRET_ACCESS_KEY=... \
  go test -tags cloud -run TestCloudBucketR2 ./s3
```

See [docs/cloud-tests.md](docs/cloud-tests.md) for credential and permission
details.

## Running in production

Every blobster utility is a polling loop over blob-storage requests, so
capacity, latency, and cost come down to poll rates, per-operation request
counts, and the backend's per-key/per-prefix rate limits.
[`docs/production.md`](docs/production.md) is the production guide: the
request-cost model, scalability ceilings and how to tune around them (lock
contention, queue head-window sizing and sharding, tail/watcher scan costs),
performance catches, the security model (IAM is the enforcement boundary;
leases assume synced clocks; no fencing tokens — handlers must be idempotent),
and a pre-production checklist.
