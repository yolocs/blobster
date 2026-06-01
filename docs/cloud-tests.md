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

`TestCloudCrossRegionCopy` runs as part of the same package. By default it uses
the same bucket with separate prefixes, which still exercises GCS `rewrite` and
the `CopyOperation` handle. Set `BLOBSTER_GCS_XCOPY_DEST_BUCKET` to copy into a
different bucket; if that bucket is in a different location, the test becomes a
genuine cross-region copy.

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

`TestCloudCrossRegionCopy` runs as part of the same package. By default it uses
the same bucket with separate prefixes, which still exercises server-side
`CopyObject` and the `CopyOperation` handle. Set `BLOBSTER_S3_XCOPY_DEST_BUCKET`
to copy into another bucket; set `BLOBSTER_S3_XCOPY_DEST_REGION` when the
destination bucket is in a different region.

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

## R2 (Cloudflare)

R2 is S3-compatible and reuses the `s3` driver, so its cloud test lives in the
same package (`TestCloudBucketR2`). Run:

```sh
BLOBSTER_R2_BUCKET=my-bucket \
  BLOBSTER_R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com \
  BLOBSTER_R2_ACCESS_KEY_ID=... BLOBSTER_R2_SECRET_ACCESS_KEY=... \
  go test -tags cloud -run TestCloudBucketR2 ./s3
```

Optional `BLOBSTER_R2_PREFIX=blobster/manual/` forces all test objects under a
chosen prefix. Unlike the AWS test, the R2 test takes credentials explicitly
(R2 access key id + secret) and sets `BaseEndpoint`, region `auto`, and the
checksum config below, rather than relying on the ambient AWS chain.

The test configures the client with `RequestChecksumCalculation` and
`ResponseChecksumValidation` set to *when-required*. This is mandatory:
`aws-sdk-go-v2` otherwise sends a CRC32 request checksum that R2 rejects
(`Header 'x-amz-checksum-algorithm' with value 'CRC32' not implemented`), failing
every write. Application code building an R2-backed `s3.Bucket` must set the same
options on its client.

R2 honors conditional writes (`If-None-Match`/`If-Match` on PUT and COPY) but does
**not** document `If-Match` on DeleteObject, so the conditional-delete part of the
conformance suite is the empirical check for whether the lock's safe-release
guarantee holds on R2 — expect that case to surface the gap. R2 also has no
S3-style regions, so there is no cross-region copy to exercise. The credential
needs permission to head the bucket and to list/create/read/copy/delete objects.

## Azure

Run:

```sh
BLOBSTER_AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net" \
  BLOBSTER_AZURE_CONTAINER=my-container go test -tags cloud ./azureblob
```

Optional:

```sh
BLOBSTER_AZURE_PREFIX=blobster/manual/ \
  BLOBSTER_AZURE_CONNECTION_STRING=... BLOBSTER_AZURE_CONTAINER=my-container go test -tags cloud ./azureblob
```

`TestCloudCrossRegionCopy` runs as part of the same package. By default it uses
the same container with separate prefixes, which still exercises `Copy Blob` and
the `CopyOperation` handle. Set
`BLOBSTER_AZURE_XCOPY_DEST_CONNECTION_STRING` and
`BLOBSTER_AZURE_XCOPY_DEST_CONTAINER` to copy into another storage account; that
path also verifies that the source client can mint the read SAS needed for a
cross-account copy.

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
