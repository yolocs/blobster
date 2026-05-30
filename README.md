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
  writes, streaming multipart uploads, server-side copy, presigned URLs)
- `azure` driver built from a caller-owned `*container.Client` (conditional
  writes, streaming block-blob uploads, async server-side copy, SAS URLs)
- shared conformance tests plus GCS, S3, and Azure cloud tests behind the
  `cloud` build tag

Planned:

- multipart upload, distributed locks, and cross-region copy helpers

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

For tests:

```sh
go test ./...
go test -race ./...
```

Cloud-backed tests:

```sh
BLOBSTER_GCS_BUCKET=my-test-bucket go test -tags cloud ./gcs
```

See [docs/cloud-tests.md](docs/cloud-tests.md) for credential and permission
details.
