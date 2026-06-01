# Releasing

blobster is a Go library, so a release is **a signed semver tag** — nothing is
compiled or uploaded. The tagged source commit *is* the artifact; the Go module
proxy serves it and consumers fetch it with `go get`. There is no GoReleaser,
no binaries, no archives.

## Versioning

We follow [Semantic Import Versioning](https://go.dev/ref/mod#versions).

- While the module is `v0.x`, the API carries **no compatibility guarantee** —
  breaking changes may land in any `v0.minor` bump.
- `v1.0.0` is the commitment to a stable API.
- Only `v2.0.0`+ requires a `/vN` import-path suffix; nothing to do before then.

## Cutting a release

1. Make sure `main` is green (Unit Tests + Static Checks) and the public API is
   what you want to freeze for this version.
2. Tag the release commit with a signed, annotated tag and push it:

   ```bash
   git tag -s v0.1.0 -m "v0.1.0"
   git push origin v0.1.0
   ```

   Use a pre-release suffix (e.g. `v0.2.0-rc1`) for release candidates; the
   release workflow marks those as GitHub pre-releases automatically.
3. The [`Release`](../.github/workflows/release.yml) workflow runs on the tag:
   it re-verifies the build, `go vet`, `go test -race ./...`, and
   `govulncheck`, then publishes a GitHub Release with auto-generated notes
   (grouped per [`.github/release.yml`](../.github/release.yml)) and warms the
   module proxy.

## Verifying

```bash
GOPROXY=proxy.golang.org go list -m github.com/yolocs/blobster@v0.1.0
```

A successful resolve means the version is live and importable.
