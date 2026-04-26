# Monitoring guide

The Flight RPC server exposes Prometheus metrics so you can watch
ingestion health, hot-cache behavior, the S3 cold tier, and client
subscriptions from a standard Prometheus + Grafana stack.

## Endpoint

| | |
|---|---|
| Port | `METRICS_PORT` env var (default `9091`) |
| Path | `/metrics` |
| Format | Prometheus text exposition (v0.0.4) |
| Implementation | `io.prometheus.client.exporter.HTTPServer` (simpleclient 0.16.0) |

```bash
curl -s http://server:9091/metrics
```

`server.jar` registers JVM/process metrics via `DefaultExports.initialize()`
in addition to the application metrics below. The one-shot
`backfill.jar` does **not** expose a metrics endpoint — observe it via
its stdout/stderr logs (chunk-level progress + per-chunk timings).

## Mental model

Metric names follow `flight_<subsystem>_<thing>` and group cleanly by
the four moving parts in the server:

```
   WSS newHeads ─► [ingestor] ──► [hot cache] ──► [archive sweep] ──► S3
                       │              ▲                                ▲
                       │              │                                │
                       │              └──── [subscriptions] ◄──────────┘
                       │                          (clients)            │
                       └─ HTTP RPC fetch                       cold-tier read
```

- **`flight_ingestor_*`** — forward fetch loop driven by `eth_subscribe("newHeads")`.
- **`flight_cache_*`** — RocksDB hot-cache reads + the prune floor.
- **`flight_archive_*`** — S3 uploads (sweep) and cold-tier reads.
- **`flight_subscription_*`** — per-client Flight streams.

---

## Custom application metrics

All custom metrics are defined in
`server/src/main/java/net/broscorp/web3/metrics/Metrics.java`. Histogram
buckets are Prometheus simpleclient defaults
(`.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10`)
unless noted otherwise.

### Ingestion (forward fetch from RPC)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `flight_ingestor_head_block` | Gauge | — | Latest chain head observed via WSS `newHeads` (or watchdog HTTP poll on WSS gap). |
| `flight_ingestor_committed_block` | Gauge | — | Highest block number written contiguously to the hot cache. Lag = `head − committed`. |
| `flight_ingestor_pending_depth` | Gauge | — | Completed block fetches buffered, waiting for the next contiguous block to commit in order. |
| `flight_ingestor_blocks_committed_total` | Counter | — | Blocks successfully committed (forward + backward backfill loop). |
| `flight_ingestor_fetch_retries_total` | Counter | — | Block-fetch retry attempts (RPC errors, exponential backoff). |
| `flight_ingestor_commit_errors_total` | Counter | — | Failures inside the commit loop (RocksDB write or serialization). |
| `flight_ingestor_fetch_duration_seconds` | Histogram | — | Latency of a successful full-block fetch (`eth_getBlockByNumber + eth_getLogs + eth_getBlockReceipts`). |

### Hot cache (RocksDB)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `flight_cache_prune_floor` | Gauge | — | Exclusive floor — blocks with `number < pruneFloor` are no longer in the hot cache (must come from S3). |
| `flight_cache_reads_total` | Counter | `dataset` (`blocks`\|`logs`), `result` (`loaded`\|`waited`\|`pruned`\|`backfilling`) | Read attempts against the hot cache, by outcome. See result legend below. |
| `flight_cache_wait_duration_seconds` | Histogram | `dataset` | Time a reader blocked in `getOrWait()` before the requested block was committed. |

`result` values:

- `loaded` — block was already in RocksDB; served immediately.
- `waited` — reader blocked on the condition variable until the block was committed; served from RocksDB.
- `pruned` — block is below `pruneFloor`; reader will fall through to the S3 cold tier.
- `backfilling` — block is inside the backward backfill window; not yet committed.

### Cold tier (S3 archive)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `flight_archive_uploads_total` | Counter | `status` (`success`\|`failure`) | Outcome of each archive sweep (one chunk = blocks + logs uploaded together). |
| `flight_archive_cold_reads_total` | Counter | `dataset` (`blocks`\|`logs`), `status` (`hit`\|`miss`\|`failure`) | Cold-tier reads from S3, by dataset and outcome. |
| `flight_archive_upload_duration_seconds` | Histogram, custom buckets `[0.1, 0.5, 1, 5, 10, 30, 60, 120]` s | — | Wall time to archive + upload one chunk's blocks and logs. |
| `flight_archive_cold_read_duration_seconds` | Histogram | — | Wall time to fetch a single block from S3 (downloads the chunk, extracts the row). |

### Subscriptions (Flight clients)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `flight_subscriptions_active` | Gauge | `dataset` | Currently open client subscriptions per dataset. |
| `flight_subscriptions_total` | Counter | `dataset` | Subscriptions created since process start. |
| `flight_subscription_batches_sent_total` | Counter | `dataset` | Arrow record batches pushed to clients via `putNext`. |
| `flight_subscription_errors_total` | Counter | `dataset` | Subscriptions terminated with an error (e.g. requested block still in backfill, downstream exception). |

---

## JVM and process metrics

`DefaultExports.initialize()` registers the Prometheus simpleclient
`hotspot` defaults. Most useful in practice:

| Metric | Type | Notes |
|---|---|---|
| `jvm_memory_bytes_used{area, ...}` | Gauge | Heap / non-heap usage. Watch for sustained high heap during backward backfill. |
| `jvm_memory_pool_bytes_used{pool}` | Gauge | Per-pool breakdown. |
| `jvm_gc_collection_seconds{gc}` | Summary | GC pause time per collector. |
| `jvm_threads_current`, `jvm_threads_daemon`, `jvm_threads_started_total` | Gauge / Counter | Catch thread-leak regressions in subscriptions. |
| `process_cpu_seconds_total` | Counter | Used as `rate(...)` for CPU%. |
| `process_resident_memory_bytes` | Gauge | RSS — pair with cgroup limit. |
| `process_open_fds`, `process_max_fds` | Gauge | S3 SDK + RocksDB + Flight gRPC all use FDs. |

---

## What to watch and alert on

Suggested rules. Tune thresholds for your chain's block time
(post-merge L1 = 12 s; L2s are sub-second).

### Ingestion lag

```promql
flight_ingestor_head_block - flight_ingestor_committed_block
```

- Mainnet: alert if `> 5` for more than 1 minute.
- Sustained growth = the ingestor isn't keeping up. Check
  `rate(flight_ingestor_fetch_retries_total[5m])` for RPC backoff and
  `flight_ingestor_pending_depth` for whether the bottleneck is fetch or
  commit.

### Stale head (WSS likely dropped)

```promql
time() - flight_ingestor_head_block_timestamp_seconds  # see note
```

The gauge itself doesn't include a freshness signal; the simpler check
is on the rate:

```promql
rate(flight_ingestor_blocks_committed_total[2m]) == 0
```

Watchdog reconnects on its own; the alert tells you it failed to.

### Commit failures

```promql
rate(flight_ingestor_commit_errors_total[5m]) > 0
```

Always page-worthy — RocksDB write failures or serializer crashes. Pair
with logs from `BlockchainIngestor`.

### Cold-tier health

```promql
rate(flight_archive_cold_reads_total{status="miss"}[10m])
  / ignoring(status) sum without (status) (rate(flight_archive_cold_reads_total[10m]))
```

A sudden miss-rate jump usually means **mismatched `S3_BUCKET` /
prefix between server and backfill** (see `configuration.md`, "Common
mistakes"). Also alert on:

```promql
rate(flight_archive_cold_reads_total{status="failure"}[5m]) > 0
rate(flight_archive_uploads_total{status="failure"}[5m]) > 0
```

### Slow archive sweep

```promql
histogram_quantile(0.95, rate(flight_archive_upload_duration_seconds_bucket[10m]))
```

Custom buckets top out at 120 s; chunks taking minutes signal S3
throttling, network problems, or a too-large chunk.

### Hot-cache reader stalls

```promql
histogram_quantile(0.95, sum by (le, dataset) (rate(flight_cache_wait_duration_seconds_bucket[5m])))
```

Persistent multi-second waits = readers are running ahead of ingestion
(reading blocks faster than they commit). Usually a client / chain
mismatch, not a server bug.

### Subscription health

```promql
rate(flight_subscription_errors_total[10m])
flight_subscriptions_active            # leak indicator
```

`flight_subscriptions_active` should track real client count; a
monotonically-rising line means the dec on close isn't firing — open a
bug.

---

## Useful PromQL one-liners

```promql
# Ingestion rate (blocks/sec, smoothed)
rate(flight_ingestor_blocks_committed_total[1m])

# Fetch p50 / p95 / p99 latency
histogram_quantile(0.50, rate(flight_ingestor_fetch_duration_seconds_bucket[5m]))
histogram_quantile(0.95, rate(flight_ingestor_fetch_duration_seconds_bucket[5m]))
histogram_quantile(0.99, rate(flight_ingestor_fetch_duration_seconds_bucket[5m]))

# Hot-cache hit ratio per dataset
sum by (dataset) (rate(flight_cache_reads_total{result="loaded"}[5m]))
  / sum by (dataset) (rate(flight_cache_reads_total[5m]))

# Cold-read fall-through rate per dataset
sum by (dataset) (rate(flight_cache_reads_total{result="pruned"}[5m]))

# Retention window in blocks (live)
flight_ingestor_committed_block - flight_cache_prune_floor

# Fan-out: batches per active subscription
sum by (dataset) (rate(flight_subscription_batches_sent_total[1m]))
  / on (dataset) flight_subscriptions_active
```

---

## Scrape config example

```yaml
scrape_configs:
  - job_name: flight-rpc
    scrape_interval: 15s
    static_configs:
      - targets: ['flight-server-1:9091', 'flight-server-2:9091']
        labels:
          chain: ethereum
```

Histogram buckets are exposed as `_bucket` series, so the
`histogram_quantile()` queries above work without further config.
