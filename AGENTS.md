# AGENTS.md

How we work on **blobster** — the principles and conventions for changing this
codebase. It is deliberately high level. The detailed design lives in
[`docs/architecture.md`](docs/architecture.md); the planning backlog lives in
[`docs/roadmap.md`](docs/roadmap.md); committed work lives in GitHub issues.

blobster is a Go library of **cloud-agnostic utilities built solely on blob
storage**. It models a blob backend as a flat keyspace of string keys — read,
write, list, delete, attributes, server-side copy — and layers higher utilities
on that one substrate: a distributed lock, cross-region copy, a blob-backed
work queue, and planned parallel multipart upload. The aim is that a service
which already runs an object store needs *nothing else* — no separate lock
service, no queue broker, no metadata database — to get these primitives. Read
`docs/architecture.md` before making non-trivial changes.

## Think from first principles

Derive decisions from the problem and the architectural invariants, not from
precedent or from what some document literally says. The invariants in
`docs/architecture.md` (blob storage is the only dependency, explicit
construction over ambient config, a prefix is the root, capabilities are
explicit and discoverable, conditional writes are the one coordination
primitive) are the fixed points; reason forward from them. When a requirement
seems off, surprising, or more complex than the problem warrants, question it
before building it — the cheapest code is the code you talk yourself out of
writing. Prefer the simplest design that honors the invariants.

## Tracking work: backlog, issues, design

Three places, three jobs — don't conflate them:

- **`docs/roadmap.md` is the backlog.** High-level direction and the space of
  features we *might* build, kept deliberately loose so it is cheap to revise.
  It is not a commitment and not a schedule — ideas live here until they are
  ready to become real work.
- **GitHub issues are committed work.** When a backlog item is concrete enough
  to build, open an issue that carries its own scope, dependencies, and
  acceptance criteria. An issue is "what we know we're going to do next"; the
  roadmap is "what we're considering." Keep status in the issue.
- **`docs/architecture.md` is the design source of truth** — what the pieces
  are, how they fit, and the invariants that hold them together.

Treat each issue with a grain of salt: it is a snapshot of intent and can be
stale. Reconcile it against the architecture doc and the actual code; if
first-principles thinking points to a better path, take it and update the issue
and the architecture doc to match.

## Keep the architecture doc alive

`docs/architecture.md` is a living document. When behavior or structure changes,
update it in the same change — a stale design doc is worse than none. If you
discover the doc and the code disagree, resolve it (fix one, note why) rather
than leaving the contradiction. New cross-cutting design decisions belong there,
not buried in a commit message or an issue comment.

## Coding principles

- **Go only**, unless there's a strong reason. Follow Effective Go.
- **Explicit, boring code over clever abstractions.** No premature generality;
  don't build for hypothetical future requirements.
- **Explicit construction, caller-owned clients.** A driver is built from a
  pre-configured native SDK client the caller passes in
  (`s3.New(client, bucket, …)`). blobster never registers drivers through
  blank-import side effects, never reads ambient cloud configuration, and never
  closes a client it did not open. The caller stays in full control of how the
  native client is configured (endpoints, credentials, retry, timeouts).
- **Respect the dependency rule.** The root `blobster` package defines the
  interfaces, shared types, the lease lock, the work queue, and the
  cross-region copy handle; drivers (`mem`, `file`, `s3`, `gcs`, `azureblob`)
  import only the root package; the planned `multipart` utility will depend on
  the root interfaces, never on a concrete driver; and no driver imports
  another. The arrows point one way.
- **Capabilities are explicit; never assume one.** The base `Bucket` interface
  is the common denominator. Anything a backend may or may not support is an
  optional interface plus a `Capabilities()` descriptor. Feature-gate by
  type-asserting the optional interface or checking the descriptor — never call
  a capability you have not confirmed.
- **Conditional writes are the coordination primitive.** Locks, the work queue,
  and any future CAS-based utility build on one optimistic-concurrency
  operation. Don't reach for a second coordination mechanism; if blob storage
  can't express it with conditional writes, question whether it belongs in
  blobster.
- **Stream; don't buffer.** Blobs can be large. Prefer streaming (`io.Copy`,
  range reads, multipart parts) over `io.ReadAll` into a `[]byte`. Hash during
  the streamed write, not in a second buffered pass.
- **Security and correctness first.** Validate at system boundaries (untrusted
  keys, caller input); trust internal invariants. Never log credentials or
  signed URLs.
- **Comments explain why, not what.** Default to none; add one only when a
  constraint or hazard isn't obvious from the code.

## Testing standards (non-negotiable)

- **Unit + integration for every feature, in the same change.** Tests are not a
  follow-up.
- The **`mem` and `file` drivers are first-class implementations, not mocks.**
  They implement the same interface and the same conditional-write semantics as
  the cloud drivers, so tests exercise real code paths. Do **not** write a mock
  `Bucket` for storage behavior — run the real layers against `mem` (and where
  relevant `file` via `t.TempDir()`).
- Integration tests that need a real cloud backend live behind a build tag and
  are skipped by default; the default `go test ./...` runs everything against
  `mem`/`file`.
- `t.Parallel()` and `t.Context()` in every test, unless a documented
  process-global exception applies.
- Table-driven tests for multiple inputs; compare with `cmp.Diff`.
- Cover the hard cases: conditional-write races (two writers, `IfNotExists`),
  lock contention, lease expiry and takeover, lost-lease signaling,
  release-after-expiry safety, queue redelivery, Nack, max-receives
  dead-lettering, multipart part concurrency and abort/cleanup, cross-region
  copy polling and cancellation, range reads, and listing with delimiters.
- Every change: `go test -race ./...`.

## Build and commit

- Use `go test -race ./...` for the required local verification.
- **Sign off every commit** (`git commit -s`) — we keep a DCO trail.

## Pull requests and labels

Release notes are generated from merged PRs, bucketed by label per
[`.github/release.yml`](.github/release.yml). **Every PR must carry exactly one
category label** so it lands in the right section of the next release's notes:

- `breaking` — backwards-incompatible API change (callers must change code).
- `feature` / `enhancement` — new capability or improvement.
- `fix` / `bug` — bug fix.
- `docs` / `documentation` — documentation only.
- `ignore-for-release` — excluded from notes entirely (e.g. CI/chore, internal
  refactors with no user-facing effect).

An unlabeled PR still ships, but falls into "Other Changes" — avoid that. Pick
`breaking` whenever the change alters the public API surface; that section is
the one consumers read first when deciding whether to upgrade.
