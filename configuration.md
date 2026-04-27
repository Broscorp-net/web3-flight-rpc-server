# Configuration guide

Everything in this repo is driven by environment variables. This guide
covers both runtime executables — the **Flight RPC server** (`server.jar`,
long-running) and the **batch backfill** (`backfill.jar`, one-shot) — and
how their settings interact.

## Mental model

Two cooperating processes share **one S3 bucket**:

```
                  +---------------------+
                  |  RPC node (HTTP/WS) |
                  +----------+----------+
                             |
              +--------------+--------------+
              |                             |
       fetch  v                       fetch v
   +------------------+          +-------------------+
   |  Flight server   |          |  Backfill job     |
   |  (long-running)  |          |  (one-shot)       |
   |                  |          |                   |
   |  RocksDB hot     |          |   (no cache)      |
   |   cache          |          |                   |
   +--------+---------+          +---------+---------+
            |                              |
            | retention age-out            | direct write
            |     + archive sweep          |
            v                              v
            +-------------+----------------+
                          |
                          v
                   +-------------+
                   |   S3 cold   |
                   |   archive   |
                   +-------------+
                          ^
                          |  cold-tier read
                          |  (server only)
                          +
```

Both writers must:
- target the **same `S3_BUCKET` (incl. prefix)** so chunks land in one place.
- write to **disjoint block ranges** — the server writes blocks aging out of
  retention; the backfill writes blocks the server will never reach (i.e.
  ranges below the server's current `pruneFloor`).

If you mis-configure either, you don't corrupt anything (chunk format is
identical, last-write-wins on the same key), but you waste RPC + S3 PUTs.

---

## Flight RPC server (`server.jar`)

### Required

| Var | Purpose |
|---|---|
| `WEBSOCKET_NODE_URL` | WS endpoint for `eth_subscribe("newHeads")`. Drives forward ingestion. |
| `HTTP_NODE_URL` | HTTP endpoint for `eth_getBlockByNumber` / `eth_getLogs` / `eth_getBlockReceipts`. |

### Networking

| Var | Default | Purpose |
|---|---|---|
| `FLIGHT_PORT` | `8815` | Arrow Flight gRPC listener (client-facing). |
| `METRICS_PORT` | `9091` | Prometheus exporter (`/metrics`). |

### Hot cache

| Var | Default | Purpose |
|---|---|---|
| `DB_PATH` | `rocksdb_cache` | RocksDB directory. **Must point at persistent storage** for the cache to survive restarts. Anything ephemeral means RPC re-backfill on every restart. |
| `INITIAL_BLOCK` | (latest head) | Starting block for a **fresh cache only**. Ignored on warm restart — resume always continues from `lastIngestedBlock + 1`. |
| `BACKFILL_BLOCKS` | `0` | On a fresh cache only, also fill the previous N blocks backward into the hot cache. Reads from S3 archive first (if configured), falls back to RPC on miss. Ignored on warm restart. |

### Retention + cold tier

| Var | Default | Purpose |
|---|---|---|
| `RETENTION_BLOCKS` | (unset = keep forever) | Hot-cache retention window. **Must be a multiple of 1000** (the archive chunk size). When set, blocks older than `lastIngestedBlock - RETENTION_BLOCKS` are eligible for the archive sweep + prune. |
| `ARCHIVE_MODE` | `optional` | `off` / `optional` / `required`. See "Archive modes" below. |
| `S3_BUCKET` | — | Bucket name. Accepts `bucket` or `bucket/prefix`. **Must match the backfill's `S3_BUCKET`** so cold reads find the back-filled chunks. |
| `S3_REGION` | `us-east-1` | AWS region. |
| `AWS_ACCESS_KEY` | — | Required if S3 is in use. |
| `AWS_SECRET_KEY` | — | Required if S3 is in use. |

### Archive modes

| Mode | S3 configured? | Behavior |
|---|---|---|
| `off` | irrelevant | No S3 writes; pruned data is **lost permanently**. Dev only. |
| `optional` | yes | S3 writes + cold reads. Same as `required`. |
| `optional` | no | Warns at startup; behaves like `off`. Pruned data lost. |
| `required` | yes | S3 writes + cold reads. |
| `required` | no | **Startup fails fast.** Use this in prod. |

`ARCHIVE_MODE=off` with `RETENTION_BLOCKS` set logs a loud warning at
startup — you're explicitly opting into permanent data loss for blocks
older than retention.

### Chunk alignment on fresh + warm starts

Archive chunks are always written on `CHUNK_SIZE` (1000-block) boundaries:
`0_1000.arrow`, `1000_2000.arrow`, … The server enforces alignment in
two places at startup so that misaligned keys don't get added to S3:

1. **Fresh cache.** If `INITIAL_BLOCK` (or current head, if unset) is
   not a multiple of `CHUNK_SIZE`, `backfillFloor` is rounded **down**
   to the chunk boundary. The startup backfill loop fills the small
   pre-`startFrom` range from S3 (if a prior aligned chunk exists) or
   from RPC. Cost: at most `CHUNK_SIZE - 1` extra blocks of backfill
   (so up to ~1000 RPC calls), one-time, only on first start. Logged
   as `Aligning fresh-cache backfillFloor: X -> Y …`.
2. **Warm cache** with a misaligned `pruneFloor` inherited from an
   older server run that wrote misaligned chunks: `archiveDispatchedUpTo`
   is rounded **up** to the next chunk boundary. The cache blocks
   between the old `pruneFloor` and the new aligned floor are kept
   hot until the next sweep, then pruned without producing a new
   aligned chunk for that partial range. Logged at `WARN`. Those
   blocks typically still exist in legacy misaligned chunks in S3
   from the previous run; a future listing-based read index is needed
   to recover them via the formula-based read path.

If you see the alignment `WARN` at startup, your bucket has legacy
misaligned chunks (pre-fix server runs). New writes will be clean
going forward, but the misaligned objects in S3 are best treated as
unreadable until the listing-index work lands.

### Memory + disk during the archive sweep

Each sweep streams one chunk through to S3 — it does **not** buffer the
chunk on heap. Per-block Arrow vectors are written to a temp file under
`java.io.tmpdir` and uploaded directly from disk. Peak per-sweep is
roughly:

- one block's vectors live in heap at a time (a few MB on mainnet);
- two temp files under `java.io.tmpdir` (one per dataset), typically
  50–200 MB each on high-activity ranges. Both are deleted after the
  upload (success or failure).

If the sweep does OOM anyway — e.g. heap is sized below what an
in-flight RocksDB read + Arrow allocator footprint needs — the ingestor
**halts the JVM with exit 137** rather than swallowing the error. Letting
the process limp on past an OOM is unsafe (next sweep retries the same
chunk, blocks pile up past `RETENTION_BLOCKS`, hot cache grows without
bound). The orchestrator should restart the container; a clean process
will resume from the persistent RocksDB cache and try the same chunk
again with a fresh heap. Make sure the temp dir isn't on a tiny
ephemeral overlay if you're running in a container.

### Memory + disk during cold-tier reads

Cold-tier reads (subscriptions hitting blocks below `pruneFloor`) use
the same disk-backed pattern: the chunk object is streamed from S3
directly to a temp file under `java.io.tmpdir`, then a forward-only
cursor (`ArchiveManager.ChunkReader`) walks the chunk's Arrow IPC
batches as the subscription consumes blocks. Each subscription holds at
most one open chunk reader at a time and downloads each chunk **once**;
consecutive in-chunk reads reuse the same temp file. The reader (and
its temp file) is closed when the subscription crosses a chunk boundary,
transitions back to the hot cache, or terminates.

Per active subscription in a cold segment:

- one chunk file under `java.io.tmpdir` (typically 50–200 MB), deleted
  on chunk-boundary crossing or subscription close;
- one Arrow batch's vectors in heap at extraction time.

The backward backfill loop (fresh-cache `BACKFILL_BLOCKS` warmup) uses
the same per-chunk reader pattern: each chunk is opened once, all
in-range blocks within it are committed to RocksDB before moving to the
next chunk down. If a chunk is missing from S3 or has gaps, the loop
falls back to RPC for the affected blocks.

Cold reads and archive writes share one `s3-archive` thread; many
concurrent cold subscriptions queue against each other (and against
archive sweeps). Splitting these into separate executor pools is
tracked as a follow-up.

### JVM

| Var | Default | Purpose |
|---|---|---|
| `JAVA_OPTS` | `--add-opens=...`, `-Djava.net.preferIPv4Stack=true` | Set in the Dockerfile. Add `-Xmx<n>g` here if you want to pin heap; otherwise the JVM defaults apply (Java 21 honors cgroup memory limits when running in a container). |

---

## Backfill job (`backfill.jar`)

### Required

| Var | Purpose |
|---|---|
| `BACKFILL_FROM_BLOCK` | Inclusive lower bound of the range to load. |
| `BACKFILL_TO_BLOCK` | Inclusive upper bound. The actual processed range is **floor-aligned to chunk boundaries** (1000) on both ends; partial chunks at the edges are left for the live server. |
| `S3_BUCKET` | Same bucket+prefix the server uses for cold reads. |
| `AWS_ACCESS_KEY` | |
| `AWS_SECRET_KEY` | |

### Source selection

| Var | Default | Purpose |
|---|---|---|
| `BACKFILL_SOURCE` | `rpc` | `rpc` or `bigquery`. BigQuery is currently a stub — throws on use. |
| `HTTP_NODE_URL` | — | Required when `BACKFILL_SOURCE=rpc`. Use an **archive-tier provider** for historical ranges; consumer tiers will rate-limit hard. |

### Throughput / behavior

| Var | Default | Purpose |
|---|---|---|
| `BACKFILL_FETCH_PARALLELISM` | `8` | Concurrent block fetches inside one chunk. Also caps per-chunk heap usage — at most this many `FullBlockData` objects are alive at once (see "Memory" below). |
| `BACKFILL_MAX_RPS` | `0` (disabled) | Token-bucket rate limit on outgoing RPC calls. Each block fetch issues 3 calls (`eth_getBlockByNumber` + `eth_getLogs` + `eth_getBlockReceipts`), so set this to ~3× your provider's allowed RPS. `0` disables the limiter — only `BACKFILL_FETCH_PARALLELISM` throttles. |
| `BACKFILL_SKIP_EXISTING` | `true` | Before assembling, the job does a `HEAD` against S3. If both `blocks/` and `logs/` chunks exist, the chunk is skipped. Set `false` to force re-upload. |

### S3

| Var | Default | Purpose |
|---|---|---|
| `S3_REGION` | `us-east-1` | |

### Memory

The backfill streams blocks through to S3 — it does not buffer a whole
chunk on heap. At any moment, up to `BACKFILL_FETCH_PARALLELISM`
`FullBlockData` objects are alive in the producer window, plus two open
Arrow IPC writers backed by temp files in `java.io.tmpdir`. Per-block
arrow conversion buffers are released as soon as the batch is appended to
the output stream.

Tune `BACKFILL_FETCH_PARALLELISM` to cap heap. As a rule of thumb, a
post-merge mainnet block parses to ~2–10 MB of `FullBlockData`, so
parallelism `8` ⇒ roughly 80 MB peak from in-flight blocks (plus Arrow
writer buffers and JVM overhead). Lower it (e.g. `2`) on small
containers; raise it (e.g. `32`) when you have RAM and an archive-tier
provider that can keep up.

Disk: up to two chunk files at once per chunk under `java.io.tmpdir`,
typically 50–200 MB each on high-activity ranges. Files are deleted after
each chunk uploads (or after a chunk aborts). Make sure the temp dir
isn't on a small ephemeral overlay if you're running in a container.

---

## How the settings interact

### 1. `RETENTION_BLOCKS` controls the live archive sweep

The server triggers an archive + prune when:

```
committedBlock >= archiveDispatchedUpTo + 1000 + RETENTION_BLOCKS
```

So:

- **Smaller `RETENTION_BLOCKS`** → blocks reach S3 sooner, hot cache stays small,
  but cold-tier reads are taken more often (slower than hot).
- **Larger `RETENTION_BLOCKS`** → fewer cold reads, but the hot cache grows
  on disk; restart still resumes from `lastIngestedBlock+1` regardless.
- **Unset** → no archiving ever; hot cache grows without bound. Fine for a
  dev cache, never for prod.

The `+ 1000` in the formula is the chunk size — the sweep waits for a full
1000-block chunk to fall outside the retention window before writing.
That's why `RETENTION_BLOCKS` must be a multiple of 1000.

### 2. `INITIAL_BLOCK` and `BACKFILL_BLOCKS` only fire on a fresh cache

A "fresh cache" is one where RocksDB's `META/lastBlock` is unset (no
previous run). On warm restart (persistent storage retains the DB):

- `INITIAL_BLOCK` is ignored — server resumes from `lastIngestedBlock + 1`.
- `BACKFILL_BLOCKS` is ignored — `pruneFloor` and `forwardStart` already
  reflect the previous run's window.

This is intentional: warm restart is **the cheap path**, and it shouldn't
re-do backfill work just because you redeployed with the same env.

### 3. `BACKFILL_BLOCKS` is the cheap way to make a server "remember" history

On a fresh cache with `BACKFILL_BLOCKS=N`:

- Forward ingestion starts from `startFrom` (= `INITIAL_BLOCK` or current
  head).
- A backward backfill loop fills `[startFrom - N, startFrom)` into the hot
  cache.
- Each backfill block first tries `S3_BUCKET` (if `ARCHIVE_MODE != off`),
  then falls back to RPC.

If the batch backfill job has pre-populated `[0, head)` in S3, a server
brought up with a large `BACKFILL_BLOCKS` and the same `S3_BUCKET` recovers
its hot cache **almost entirely from S3** — fast, doesn't burn RPC quota.

### 4. Backfill range vs. server `pruneFloor`

The batch backfill must write only ranges the server **won't reach**.
Picture two block axes for a running server:

```
   0                          pruneFloor                 head
   |--------------------------|---------------------------|
        (server cannot write here)     (server writes here)
```

- The server's archive sweep writes chunks that age past
  `lastIngestedBlock - RETENTION_BLOCKS`. So once a chunk has been pruned
  from the hot cache, the server will not re-write it — even if you delete
  it from S3.
- The backfill should target `[0, T)` where `T` is comfortably below the
  server's current `pruneFloor`. A safe choice is `T = currentHead -
  10 × RETENTION_BLOCKS`.
- Picking `T` too high risks the server's archive sweep landing on the same
  chunk key while the backfill is uploading. Both produce identical bytes
  for a given chunk (same code path through `Converter` + `StreamingChunkWriter`),
  so it's not corruption — just wasted work.

### 5. `S3_BUCKET` must be byte-identical between server and backfill

Including the optional `/prefix`. The cold-tier read path computes:

```
key = "<keyPrefix>/<dataset>/<startBlock>_<endBlock>.arrow"
```

If the server is reading `prod/ethereum/blocks/...` and the backfill wrote
to `prod/ethereum-archive/blocks/...`, cold reads will miss every time and
fall through to the RPC fallback (or fail, depending on
`ARCHIVE_MODE`).

---

## Common mistakes

- **`DB_PATH` on ephemeral storage** → "warm restart" is actually always
  cold. Watch for `Cache initialized: lastIngestedBlock=-1` after every
  redeploy — that means RocksDB couldn't find the old DB.
- **`RETENTION_BLOCKS` not a multiple of 1000** → server fails fast at
  startup with `RETENTION_BLOCKS (...) must be a multiple of ARCHIVE_CHUNK_SIZE
  (1000)`. Round it.
- **`ARCHIVE_MODE=optional` + `RETENTION_BLOCKS` set + S3 not configured**
  → server starts (mode is `optional`) but pruned blocks are lost forever.
  Use `required` in prod to fail fast on missing creds.
- **Backfill range overlapping the server's live retention window** → both
  sides race on the same chunk; harmless but wastes work. Pick `T` well
  below `pruneFloor`.
- **Mismatched `S3_BUCKET` prefixes** → cold reads silently miss.
  `flight_archive_cold_reads_total{status="miss"}` is your indicator.
- **Backfill against a consumer-tier RPC provider** → mass-fetch historical
  blocks will rate-limit you within minutes. Use an archive tier; set
  `BACKFILL_MAX_RPS` to ~3× your provider's request-rate cap (each block
  costs 3 RPC calls), and tune `BACKFILL_FETCH_PARALLELISM` for memory.
- **OOM during backfill** → drop `BACKFILL_FETCH_PARALLELISM` (caps the
  in-flight `FullBlockData` window). The chunk itself is no longer
  buffered on heap, so the only knob that affects backfill RAM is this
  one.

---

## Concrete example

**Long-running server (per chain):**

```env
WEBSOCKET_NODE_URL  = wss://eth.archive.example.com
HTTP_NODE_URL       = https://eth.archive.example.com
DB_PATH             = /var/lib/flight-rpc/rocksdb_cache  # persistent
INITIAL_BLOCK       = (unset; will use current head on first start)
BACKFILL_BLOCKS     = 1000000   # ~4 months L1, used once on fresh cache
RETENTION_BLOCKS    = 100000    # ~2 weeks L1
ARCHIVE_MODE        = required
S3_BUCKET           = my-flight-archive/ethereum
S3_REGION           = us-east-1
AWS_ACCESS_KEY      = ***
AWS_SECRET_KEY      = ***
FLIGHT_PORT         = 8815
METRICS_PORT        = 9091
```

**One-shot batch backfill (run before/alongside the server):**

```env
BACKFILL_FROM_BLOCK         = 0
BACKFILL_TO_BLOCK           = 18000000     # well below currentHead - 10 * RETENTION_BLOCKS
BACKFILL_SOURCE             = rpc
BACKFILL_FETCH_PARALLELISM  = 32           # archive-tier provider; also caps RAM
BACKFILL_MAX_RPS            = 0            # disabled; rely on parallelism only
BACKFILL_SKIP_EXISTING      = true
HTTP_NODE_URL               = https://eth.archive.example.com
S3_BUCKET                   = my-flight-archive/ethereum    # SAME as server
S3_REGION                   = us-east-1
AWS_ACCESS_KEY              = ***
AWS_SECRET_KEY              = ***
```

After both run:

- Server is at chain head with the last ~2 weeks in hot cache and the
  previous ~4 months in S3 (initial `BACKFILL_BLOCKS` window — pulled from
  S3 by the backward backfill loop, which the batch job pre-populated).
- Anything below `head - BACKFILL_BLOCKS` is queryable via cold-tier reads
  to S3.
- Restarts re-use the persistent cache; no RPC re-fetch.
