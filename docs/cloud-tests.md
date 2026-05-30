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
