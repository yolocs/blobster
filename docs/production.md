# blobster — Production guide

How to run blobster-based systems in production: the request-cost model behind
every utility, the scalability ceilings and how to tune around them, the
security model and its sharp edges, and the operational catches that are easy
to miss. The design itself lives in [`architecture.md`](architecture.md); this
document assumes it and focuses on *operating* the library.

The one-sentence mental model: **every blobster utility is a polling loop over
blob-storage requests against a small number of hot keys.** There is no broker,
no server, and no push channel, so capacity, latency, and cost all reduce to
(a) how often each loop polls, (b) how many requests each iteration issues, and
(c) the backend's per-key and per-prefix rate limits. Almost every tuning knob
in the library adjusts one of those three.

## The request-cost model

Costs below are backend requests per operation, assuming the happy path. They
matter twice: against provider rate limits and against per-request pricing
(every poll is billed whether or not it finds work).

| Operation | Requests | Notes |
|---|---|---|
| `Locker.TryAcquire` (uncontended) | 3 | read record (miss) + create + version read-back |
| `Locker.Acquire` retry while held | 1 per attempt | one record read per jittered poll |
| Lock renew (background, per renew interval) | 2 | CAS write + version read-back |
| `Lock.Release` | 2–3 | record read + conditional delete |
| `Queue.Enqueue` | 1 | streamed create-only write |
| `Queue.TryReceive`, empty queue | 1 | one LIST of the head window |
| `Queue.TryReceive`, successful claim | ~5 | LIST + lease read + lease write + version read-back + payload check |
| Message renew (background, per renew interval) | 2 | CAS write + version read-back |
| `Message.Ack` | 3 | lease read + conditional lease delete + payload delete |
| `Tail.Page` | ⌈retained log / 1000⌉ | scans `msg/` from the head every call |
| `Trim` pass | 1 LIST + 1–2 deletes per removed entry | bounded at `DefaultTrimBatch` (256) removals |
| Watcher, per replicated message | 3 + payload transfer | attributes + read + create-only write, serial |

Two systematic overheads to know about:

- **Every coordination write costs an extra read.** `WriteAll` does not return
  the new version token, so the lock, queue lease, and watcher cursor all
  re-read attributes after each write to get the token for the next CAS. Budget
  2 requests per renew/claim/cursor-persist, not 1.
- **The S3 driver doubles deletes.** S3's `DeleteObject` is silent on missing
  keys, so the driver issues a `HeadObject` first to honor the `ErrNotFound`
  contract. On S3, an `Ack` is ~5 requests, a `Trim` removal up to 4.

A worked idle-cost example: 10 workers blocked in `Receive` on an empty queue
back off to the default 5 s cap → ~2 LISTs/s → ~170 k requests/day for that one
queue (order of $1/day on S3, similar magnitude on GCS Class A ops). Idle
polling is the dominant standing cost of the queue and the watcher; tune
`WithQueueReceiveBackoff` / `WithWatchPollBackoff` caps to what your latency
target actually needs.

## Backend rate limits and rough capacity

The published quotas the defaults are designed around (your account/bucket may
differ; treat everything in this section as order-of-magnitude ceilings to plan
against, not benchmarks, and leave yourself ~2× headroom):

| Raw blob ops | S3 | GCS | Azure |
|---|---|---|---|
| Writes (distinct keys) | ~3,500/s per prefix (shard prefixes for more) | ~1,000/s per bucket initially, autoscales under sustained load | ~20,000/s per account |
| Reads (distinct keys) | ~5,500/s per prefix | ~5,000/s per bucket initially | ~20,000/s per account |
| Writes to **one** key | no fixed cap; concurrent CAS races 409 | **~1/s sustained** | ~500 req/s per blob |
| Reads of one key | within prefix read budget | scales | ~500 req/s per blob |

GCS's 1 write/s per object name is the limit that shapes the library: every
lock record, lease record, and watcher cursor is a single hot object, which is
why all lease timing is second-scale. Do not set a renew interval below ~2 s or
a lease below ~5 s anywhere you might run on GCS, and do not build anything
that CAS-spins on one key. On S3, prefix partitioning ramps up gradually — a
brand-new prefix hit with a burst may throttle (503 `SlowDown`) before S3
splits it.

### Lock capacity

At the defaults (15 s lease, renew every 5 s), each held lock costs 0.2 writes/s
+ 0.2 reads/s of heartbeat; each standby contender costs ~0.67 reads/s of
polling. That gives, per backend:

| Lock | S3 | GCS | Azure |
|---|---|---|---|
| Concurrently held locks | ~15,000 per prefix (write-bound) | ~5,000 per fresh bucket | ~50,000 per account |
| Handoff rate on one lock | contention-bound, ~tens/s | **~0.5/s** (2 writes per handoff on one object) | ~100/s theoretical (per-blob cap) |
| Standby contenders on one lock | shares the prefix read budget (~8,000) | effectively unbounded | ~700 (per-blob 500 req/s) |

With heavy work (hold times of dozens of seconds), the per-lock handoff limits
never bind — handoff rate is 1/hold-time by definition. The planning number is
the aggregate heartbeat: held locks × 0.4 req/s, against the write budgets
above.

### Queue capacity with heavy handlers

Assume a 30 s handler. Per message end to end: ~10 writes + ~10 reads + 1 LIST
(enqueue 1 write; claim ≈ 1 LIST + 3 reads + 1 write; six renews ≈ 6 writes +
6 reads; ack ≈ 1 read + 2 deletes — S3 adds ~3 extra HEADs). Per-message lease
traffic stays safely under GCS's 1 write/s per object as long as renew ≥ 2 s.

**For heavy work, the head window — not the cloud — is the throughput
ceiling.** In-flight messages = rate × processing time (Little's law), leased
payloads stay listed, and available messages beyond the window are invisible —
so one queue sustains at most (effective head window ÷ processing time). S3
caps a list page at 1,000 keys (a larger `WithQueueHeadWindow` is silently
truncated) and Azure at 5,000; GCS pages internally past 1,000 at extra LIST
cost per receive.

At the **default** window of 256, a 30 s handler caps every queue at ~8 msg/s
on every backend — raising `WithQueueHeadWindow` toward the backend cap is the
first tuning step for heavy work. The table assumes that's done:

| Queue, 30 s handlers | S3 | GCS | Azure |
|---|---|---|---|
| Head-window bound (per queue) | **~30 msg/s** | ~30 msg/s (more with a >1,000 window, at extra LIST cost) | ~160 msg/s |
| Backend bound (if the window weren't binding) | ~350 msg/s per prefix | ~100 msg/s per fresh bucket | ~1,000 msg/s per account |
| Enqueue-only (broadcast log, no consumers) | ~3,500 msg/s per prefix | ~1,000 msg/s per fresh bucket | account ceiling |

The head-window bound scales inversely with handler time (60 s handlers →
~15 msg/s per queue on S3) and linearly with sharding — N queue prefixes give
N× all of the above (on GCS, until the shared bucket budget binds). At a full
1,000-message window, renew heartbeats alone add ~200 writes/s + 200 reads/s —
~20 % of a fresh GCS bucket's write budget. One more GCS note for hot broadcast
logs: time-sortable ids are sequential key names, which work against GCS's
key-range autoscaling at very high enqueue rates.

### SDK configuration

The library deliberately leaves retries to the native SDK clients (construction
invariant: the caller owns the client). **Configure your SDK retryer and
timeouts for these throttling responses** — blobster will surface what the SDK
gives up on. The lock and queue renew loops tolerate transient errors (they
retry on the next tick until the lease deadline), but a client with no retries
and long default timeouts can burn most of a lease on one stuck call: keep
per-request timeouts well under the renew interval.

## Scalability: hot spots and tuning

### The lock

A lock is one hot object. The scalability dimensions are contender count and
hold duration:

- **Contention cost.** Each blocked `Acquire` polls the record once per
  jittered retry (default 1–2 s). N contenders ≈ N/1.5 reads/s on one key —
  fine for reads, but on expiry all contenders race a CAS takeover and exactly
  one wins; the rest are billed a failed write. With many contenders (> ~50),
  raise `WithRetryInterval` so the herd thins out.
- **Renew traffic.** Holding thousands of distinct locks concurrently is a
  request-budget question, not a correctness one — see the lock capacity table
  above. Lengthen `WithLeaseDuration` (renew defaults to lease/3) for many
  long-held locks; the price is slower crash recovery — a dead holder's lock
  stays stuck for up to a full lease.
- **Don't fan many lockers into one key.** Use one lock per protected resource
  (`Acquire(ctx, key)` is per-key); the per-object write ceiling is per lock
  record, so distinct keys scale horizontally.

### The queue

The queue's ceilings, in the order you will hit them:

1. **Head-window crowding (the silent one).** `Receive` lists only the first
   `HeadWindow` keys of `msg/` (default 256), and **leased-but-unacked payloads
   stay listed until acked**. If in-flight messages + claim contention fill the
   window, available messages beyond it are invisible and workers report an
   empty queue while a backlog exists. Size `WithQueueHeadWindow` comfortably
   above (concurrent workers + expected in-flight messages). Long-running
   handlers with many workers are the classic trigger.
2. **Claim contention.** Workers start claims at a jittered offset within the
   first 16 listed keys (a fixed constant). Beyond a few dozen workers per
   queue prefix, lost claim races (each a billed failed write) grow steadily.
3. **The single-prefix request ceiling.** All of a queue's traffic lands under
   one prefix. Before you reach the backend's per-prefix limit, **shard by
   composition**: run N queues over N prefixes and spread producers/consumers
   across them — that is the designed scaling path, there is no in-library
   sharding.
4. **Idle polling cost** — see the worked example above. The floor is
   `WithQueueReceiveBackoff`'s cap; raising it trades delivery latency for
   standing cost.

Concrete throughput numbers are in the queue capacity table above. Renew
traffic scales like the lock's — 2 requests per held message per renew
interval — so either hold fewer messages concurrently or raise
`WithQueueVisibilityLease`.

### Tail, Watcher, and Trim (the broadcast-log path)

- **`Tail.Page` scans the retained log from its head every call.** The base
  `ListPage` has no start-after, so each page costs ⌈retained log / 1000⌉ LIST
  requests *even when nothing is new*. A watcher polling a 100 k-message
  retained log at the 5 s idle cap issues ~100 LISTs per poll, per follower.
  **Retention is not optional for a tailed log** — pair the leader queue with
  `WithRetention` and schedule `Trim` so the retained length (and therefore
  every follower's per-poll cost) stays bounded. Keep retained length in the
  low thousands if you can.
- **Replication is serial.** The watcher replicates one message at a time
  (≈3 requests + payload bytes each, cross-region RTTs included), so follower
  throughput is roughly 1 / (3·RTT + transfer time) messages/sec. Payload bytes
  flow *through the watcher process* (egress + ingress; provider-agnostic by
  design), so large payloads cost bandwidth on the watcher host. If the leader
  sustains more than the watcher can drain, lag grows without bound until the
  retention horizon eats the cursor — size retention from worst-case follower
  lag *plus downtime*, with a large margin.
- **`Trim` is caller-scheduled and bounded** (256 removals per pass). A
  backlog of B expired messages needs B/256 passes; schedule trims frequently
  enough that passes keep up with the enqueue rate. Concurrent trims are safe;
  gate behind the `Locker` if you want exactly one janitor.

### `mem` and `file` driver limits

Both are first-class for correctness, not for scale:

- `file` serializes *every* operation through one per-bucket mutex and its
  listing **walks the entire data tree per page** (a full listing is O(N²) in
  the object count; a queue `Receive` walks everything under the bucket even
  with a prefix). Page tokens are positional offsets, so pages can skip or
  repeat entries when objects are written/deleted between pages. Fine for
  single-host tools and tests up to thousands of objects; not a high-object-
  count or high-concurrency backend.
- `file` fsyncs file contents but not parent directories, so a host crash
  immediately after a commit can lose the rename on some filesystems. Don't
  treat it as crash-durable storage for coordination state that matters.
- `mem` listing is O(N) per page with a full sort, and every read copies the
  object. Unbounded memory by design.

## Performance and efficiency catches

- **Per-writer buffer sizes differ wildly by driver.** Streaming writes buffer
  one part/chunk in memory per open writer: the S3 upload manager defaults to
  5 MiB parts, the GCS writer to a 16 MiB chunk, Azure's `UploadStream` to
  1 MiB blocks. Many concurrent writers multiply this. `WriterOptions.BufferSize`
  sets the part/chunk/block size and `MaxConcurrency` the per-writer upload
  parallelism on all three — turn BufferSize down for many small concurrent
  writes, up (with MaxConcurrency) for large-object throughput.
- **An MD5-validated Azure write buffers the whole body in memory** (a single
  transactional Put Blob is the only way Azure can validate whole-object MD5).
  Don't set `ContentMD5` on large Azure writes. Other drivers stream while
  hashing.
- **`WriteAll` hashes and sniffs every payload.** It computes MD5 and runs
  content-type detection on each call; that is per-call CPU on hot paths (the
  lock/queue already disable sniffing internally). For high-rate small writes
  where you don't need MD5, use `Upload`/`NewWriter` with an explicit
  content type.
- **Every `Seek` on a reader is a new ranged request** (S3, GCS, Azure all
  reopen). Seek-heavy access patterns (e.g. archive extraction) belong on a
  local copy.
- **`ReadAll`/`Message.ReadAll` buffer the whole object** — prefer the
  streaming readers beyond a few MiB.
- **There is no multipart parallel-upload helper yet** (roadmap). Uploads do
  stream through each SDK's native machinery (S3 multipart manager, Azure
  blocks, GCS chunks) with `BufferSize`/`MaxConcurrency` as the knobs, but
  there is no resume across processes.

## Security

### IAM is the only enforcement boundary

Everything blobster builds — locks, queue, cursors — is **cooperative**.
Any principal with write access to the bucket can take over any lock, ack or
redeliver any message, dead-letter or trim anything, and rewind a watcher
cursor. The lock's `owner` field is advisory (it appears in records for
debugging and lets `Release` recognize its own record); it is not
authenticated. Consequences:

- Scope credentials to the narrowest prefix the workload needs (S3 policy
  prefix conditions, GCS IAM conditions / managed folders, Azure container- or
  directory-scoped SAS). A read-only follower tailing a leader's log needs
  exactly read+list on the leader's queue prefix — the `Tail`/`Watcher` design
  assumes you enforce that.
- In multi-tenant buckets, give blobster coordination state (locks, queues,
  cursors) its own prefix or bucket that application/tenant credentials cannot
  write.

### The reserved prefix is a convention, not a wall

Drivers do **not** reject reads or writes under `.blobster/`. An
attacker-controlled *key* that reaches `WriteAll`/`Delete` unchanged can name
`.blobster/locks/<name>` and corrupt or release a lock. Likewise `Sub` is plain
string concatenation: `Sub("tenants/" + name + "/")` with a `name` containing
`/` lands in another tenant's subtree, and a forgotten trailing slash makes
`tenants/alice` overlap `tenants/alicex`. The `file` driver rejects
path-escaping keys; the cloud drivers accept nearly any string (object stores
have no path semantics, so there is no traversal — but there is also no
rejection). Rules:

- **Validate untrusted key material at your boundary**: require single path
  segments (no `/`, no `..`, non-empty) for anything user-derived, exactly as
  the lock/queue/watcher validate their own names.
- Always end `Sub` prefixes with `/`.
- Never let untrusted writers share a root with your lock prefix or queue
  prefix.

### Signed URLs and SAS tokens are bearer credentials

Anyone holding one has the granted access until expiry — never log them, never
put them in error messages or traces (the library itself never logs; keep it
that way in wrappers). Defaults are 1 hour for `SignedURL` and 1 hour for the
SAS the Azure driver mints internally for cross-account `XCopyFrom`
(`WithCrossAccountSASExpiry`); shorten where workflows allow. The Azure
cross-account SAS must outlive the server-side copy, so don't shorten it below
your worst-case copy duration.

### Clocks are part of the correctness envelope

Lease deadlines are absolute wall-clock timestamps written by one host and
compared by others. A contender whose clock runs *ahead* by more than the
lease-minus-renew margin (10 s at the defaults) can judge a live lease expired
and take it over while the holder still believes it holds. Run NTP/chrony
everywhere; if you cannot bound skew under a few seconds, lengthen the lease
and keep renew at lease/3 so the margin grows with it. The same applies to the
queue's per-message leases, to `Trim`'s horizon, and to the tail's lag gate
(producer clock skew eats into `WithTailLag` — keep the lag comfortably above
skew + worst-case payload write time).

### No fencing tokens — handlers must be idempotent

This is a lease lock, not a fencing lock (see `architecture.md` for the full
rationale): a holder paused past its lease (GC, VM freeze) can resume and act
concurrently with its successor, and no in-process check can prevent that. The
`Done()`/`Err()` loss signals on locks and messages are *liveness* signals.
Production stance: keep critical sections short, make all effects idempotent,
and when the protected resource is itself a blobster object, guard the final
write with `IfMatch` on that object's version — the object's own CAS, not the
lock, is then the safety mechanism. The queue is at-least-once *by contract*,
not just under crashes: a non-crashing claim/ack race can occasionally
redeliver.

### Miscellaneous

- **Queue payloads and attributes are untrusted input** to consumers — anyone
  with write access to the prefix can enqueue. Validate/authenticate payloads
  at the application layer if producers aren't fully trusted.
- **Metadata must be ASCII-safe on S3 and GCS.** Only the Azure driver escapes
  metadata keys/values; S3/GCS pass them through as HTTP headers, where
  non-ASCII values fail or are mangled by the SDKs. Keep user metadata to ASCII
  on portable code paths.
- **MD5 here is an integrity check, not tamper-proofing.** Use bucket-level
  encryption/immutability features (configured on the caller-owned client,
  e.g. SSE headers via `BeforeWrite`) for actual security properties.
- The release pipeline runs `govulncheck`; keep SDK dependencies current —
  conditional-write behavior (notably S3's, added 2024–2025) depends on
  reasonably recent SDKs and service behavior.

## Operational catches

Sharp edges that are by design but will surprise you at 3 a.m.:

- **Always settle handles.** A dropped `Lock` without `Release`, or a `Message`
  without `Ack`/`Nack`, leaks a background renewer goroutine **and keeps the
  resource held until the process exits** — the lease never expires because the
  renewer keeps renewing. The same applies to a wedged-but-alive worker: it
  holds its one message forever (head-of-line for that message only). Watch for
  goroutine growth and for messages with abnormally old leases.
- **Orphan lease records accumulate unless retention runs.** Several documented
  crash windows (claim racing an ack, dead-letter moves) leave empty lease
  records with no payload. They are harmless to correctness and invisible to
  discovery, but they are real objects: only `Trim`'s residual sweep removes
  them. A long-lived queue **without** `WithRetention` slowly accretes them —
  enable retention even on consumer-acked queues, or sweep `lease/` yourself.
- **`Trim`'s horizon is a hard policy.** It deletes messages older than the
  horizon *even while a worker holds their lease* (the worker gets
  `ErrMessageLost`). Retention must comfortably exceed worst-case queue dwell +
  processing time — and for replicated logs, worst-case follower lag +
  downtime, or the follower falls off the log and must re-bootstrap (the
  reconciliation sweep is a roadmap item, so today that re-bootstrap is yours).
- **Caller-supplied ids opt out of the time machinery.** An `EnqueueWithID` id
  outside the `<13-digit-millis>-<suffix>` shape is never trimmed by retention,
  never lag-gated by `Tail`, and sorts at an arbitrary position. A
  non-time-sortable id on a tailed/retained queue is a permanent-resident
  message and an ordering hazard — if you supply ids, keep the time-sortable
  shape (as the watcher does).
- **`Watcher.Run` returns on any backend error.** It does not retry storage
  failures internally; run it under a supervisor that restarts it (with
  backoff). Resumption is safe by design — the durable cursor plus
  create-only re-enqueue make replays no-ops.
- **Cross-region copy handles are in-memory.** If the process exits mid-copy,
  the outcome is unobservable and (on S3's multipart path) an aborted-late copy
  cleans up best-effort within a 30 s detached window. Set lifecycle rules for
  incomplete multipart uploads on destination buckets as the backstop, and
  treat "copy finished?" as answerable only by checking the destination object.
- **Cloudflare R2: conditional delete is not honored**, and the driver cannot
  detect R2 to report it — `Capabilities()` will still claim conditional
  writes (which R2 does honor on PUT). The lock works, but `Release`'s
  "delete only if still mine" guarantee is gone; gate on knowing your backend,
  not the descriptor.
- **GCS metadata updates don't move the version token.** `UpdateMetadata`
  returns the *generation*, which a metadata-only update leaves unchanged — an
  `IfMatch` CAS taken before someone else's metadata update will still succeed
  after it. Keep CAS-protected state in object *bodies* (as the lock and queue
  do), never in metadata you also update via `UpdateMetadata`.
- **No built-in observability.** The library has no metrics, logs, or traces
  (a telemetry-free root package is deliberate; hooks are on the roadmap).
  Instrument at the seams you own: wrap the `Bucket` interface to count/time
  backend calls, watch `Done()` channels for lost leases, alert on
  `Receives()` climbing (poison messages) and on `dead/` growth, and track
  watcher cursor lag (leader head id vs. follower cursor — both are
  millisecond timestamps, so lag is a subtraction).

## Tuning quick reference

| Knob | Default | Raise when | Lower when |
|---|---|---|---|
| `WithLeaseDuration` / `WithQueueVisibilityLease` | 15 s | clock skew, slow renew path, many held locks/messages (less heartbeat) | faster crash takeover matters more than renew cost |
| `WithRenewInterval` / `WithQueueRenewInterval` | lease/3 (5 s) | reduce heartbeat request volume | rarely; never < ~2 s (GCS per-object write limit) |
| `WithRetryInterval` (lock) | 1 s | many contenders on one lock | low-latency handoff on a quiet lock |
| `WithQueueHeadWindow` | 256 | workers + in-flight messages approach it (starvation) | tiny queues where LIST size matters |
| `WithQueueReceiveBackoff` / `WithWatchPollBackoff` | 250 ms – 5 s | idle request cost too high | delivery/replication latency target is tight |
| `WithRetention` | off | — always set it on tailed/broadcast logs; floor = worst dwell + processing (+ follower lag + downtime) | shorter retained log = cheaper `Tail.Page` scans |
| `WithMaxReceives` | off (∞ redelivery) | any handler can fail persistently (poison messages) | you implement your own policy from `Receives()` |
| `WithTailLag` | 5 s | producer clock skew / slow payload commits | single in-order producer (tests) |
| `WriterOptions.BufferSize` | 5 MiB (S3) / 16 MiB (GCS) / 1 MiB (Azure) | large-object throughput (with `MaxConcurrency`) | many concurrent writers, memory-bound |
| `WithCopyPollInterval` (Azure) | 200 ms | long cross-region copies (cut poll traffic) | small same-region copies dominate |
| `WithCrossAccountSASExpiry` (Azure) | 1 h | very large cross-account copies | tighter credential exposure |
| SDK retryer/timeouts (caller-owned client) | SDK defaults | — configure throttling retries; keep per-request timeouts ≪ renew interval | — |

## Pre-production checklist

- [ ] SDK clients configured with retries suited to throttling and per-request
      timeouts well under the renew interval.
- [ ] Hosts clock-synced; lease durations sized for your worst-case skew and
      pause behavior.
- [ ] All handlers idempotent (queue is at-least-once; lock has no fencing).
- [ ] Every `Lock` released and every `Message` acked/nacked on all code paths
      (including panics — use `defer`).
- [ ] `WithQueueHeadWindow` > concurrent workers + expected in-flight messages.
- [ ] Queue sharded across prefixes before approaching per-prefix rate limits.
- [ ] `WithRetention` + scheduled `Trim` on every tailed or long-lived queue;
      retention floor accounts for follower lag + downtime.
- [ ] `WithMaxReceives` set (or an explicit `Receives()`-based policy), and
      `dead/` monitored.
- [ ] `Watcher.Run` supervised with restart + backoff; cursor lag monitored.
- [ ] IAM scoped per prefix; untrusted key material validated (single
      segments); no untrusted writer shares a root with `.blobster/` or a
      queue prefix.
- [ ] Signed URLs / SAS kept out of logs; expiries reviewed.
- [ ] Lifecycle rule for incomplete multipart uploads on S3 destination
      buckets used by `XCopyFrom`.
- [ ] Idle-poll request cost estimated (workers × LIST rate, watcher ×
      log-scan size) and acceptable.
