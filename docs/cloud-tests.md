# Cloud Integration Tests

Cloud-backed tests are excluded from the default suite and run only with the
`cloud` build tag.

## GCS

Run:

```sh
BLOBSTER_GCS_BUCKET=my-test-bucket go test -tags cloud ./gcs
```

Optional:

```sh
BLOBSTER_GCS_PREFIX=blobster/manual/ BLOBSTER_GCS_BUCKET=my-test-bucket go test -tags cloud ./gcs
```

Credentials are the standard Google Application Default Credentials used by
`cloud.google.com/go/storage`. Provide one of:

- `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json`
- a local ADC login from `gcloud auth application-default login`

The credential needs permission to read bucket attributes, list objects, create
objects, read objects, copy objects, and delete objects in `BLOBSTER_GCS_BUCKET`.
The test writes under a unique prefix named `blobster-cloud-<timestamp>/` beneath
`BLOBSTER_GCS_PREFIX` and attempts cleanup at the end.

Signed URL support is part of the driver API but is not exercised by the cloud
test yet. To use it in application code, construct the bucket with
`gcs.WithSignedURLs` and provide either a private key or a `SignBytes` function.

## S3

Run:

```sh
BLOBSTER_S3_BUCKET=my-test-bucket go test -tags cloud ./s3
```

Optional:

```sh
BLOBSTER_S3_PREFIX=blobster/manual/ BLOBSTER_S3_BUCKET=my-test-bucket go test -tags cloud ./s3
```

Credentials and region come from the standard AWS default chain loaded by
`github.com/aws/aws-sdk-go-v2/config.LoadDefaultConfig` — environment variables
(`AWS_REGION`, `AWS_ACCESS_KEY_ID`, …), a shared profile, or an instance/role
provider. The credential needs permission to head the bucket, and to
list/create/read/copy/delete objects under `BLOBSTER_S3_BUCKET`. The test writes
under a unique prefix named `blobster-cloud-<timestamp>/` beneath
`BLOBSTER_S3_PREFIX` and attempts cleanup at the end.

Conditional writes require S3's recent support: `If-None-Match` (create-only, GA
2024-08), `If-Match` on PUT (CAS, GA 2024-11), and `If-Match` on DELETE
(conditional delete, GA 2025-09). Test against a general-purpose bucket in a
region where these are available.

## Azure

Run:

```sh
BLOBSTER_AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net" \
  BLOBSTER_AZURE_CONTAINER=my-container go test -tags cloud ./azure
```

Optional:

```sh
BLOBSTER_AZURE_PREFIX=blobster/manual/ \
  BLOBSTER_AZURE_CONNECTION_STRING=... BLOBSTER_AZURE_CONTAINER=my-container go test -tags cloud ./azure
```

The test builds a `*container.Client` from the connection string via
`container.NewClientFromConnectionString`. A shared-key connection string is used
(rather than a token credential) because it also enables SAS signing, which the
signed-URL conformance case exercises. The credential needs permission to read
container properties, and to list/create/read/copy/delete blobs in
`BLOBSTER_AZURE_CONTAINER`. The container must already exist. The test writes
under a unique prefix named `blobster-cloud-<timestamp>/` beneath
`BLOBSTER_AZURE_PREFIX` and attempts cleanup at the end.

Conditional writes map to `If-None-Match: *` (create-only) and
`If-Match`/`If-None-Match` ETag conditions; all are supported on both write and
delete. `Copy` uses the asynchronous Copy Blob operation polled to completion;
same-account copies (the only kind the base `Copy` performs) typically complete
on the first poll.
