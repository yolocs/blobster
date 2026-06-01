# blobster

Cloud-agnostic utilities solely based on blob storage.

blobster starts with the same ordinary blob operations you expect from
`gocloud.dev/blob` while keeping construction explicit: callers pass in native
SDK clients, and drivers do not register themselves through import side effects.

## Status

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
  primitive

Planned:

- multipart parallel-upload helper

## Example

```go
ctx := context.Background()
client, err := storage.NewClient(ctx)
if err != nil {
	return err
}
defer client.Close()

bucket := gcs.New(client, "my-bucket", gcs.WithPrefix("app/"))
if err := bucket.WriteAll(ctx, "hello.txt", []byte("hello"), &blobster.WriterOptions{
	ContentType: "text/plain",
}, blobster.IfNotExists); err != nil {
	return err
}
```

### Work queue

`blobster.NewQueue` turns any bucket that supports conditional writes into a
competing-consumers work queue. It owns a caller-supplied prefix and stores each
message as an immutable payload plus a separate lease record; handlers must be
idempotent (delivery is at-least-once).

```go
q, err := blobster.NewQueue(bucket, "jobs/", blobster.WithVisibilityLease(15*time.Second))
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
	return msg.Nack(ctx) // return it for redelivery
}
_ = body
return msg.Ack(ctx) // processed; remove it
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
