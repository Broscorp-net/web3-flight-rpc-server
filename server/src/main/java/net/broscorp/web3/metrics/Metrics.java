package net.broscorp.web3.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

/**
 * Holds the Prometheus metric instances for the flight server.
 *
 * <p>All metrics are constructed against an injected {@link CollectorRegistry}
 * so that tests can use isolated registries without interfering with each
 * other or the default one. Production wires {@link CollectorRegistry#defaultRegistry}.
 *
 * <p>Dataset labels use the constants from {@link
 * net.broscorp.web3.service.ArchiveManager}: {@code blocks} and {@code logs}.
 */
public class Metrics {

    public final Gauge ingestorHeadBlock;
    public final Gauge ingestorCommittedBlock;
    public final Gauge ingestorPendingDepth;
    public final Gauge cachePruneFloor;
    public final Gauge subscriptionsActive;

    public final Counter ingestorBlocksCommittedTotal;
    public final Counter ingestorFetchRetriesTotal;
    public final Counter ingestorCommitErrorsTotal;
    public final Counter cacheReadsTotal;
    public final Counter archiveUploadsTotal;
    public final Counter archiveColdReadsTotal;
    public final Counter subscriptionsTotal;
    public final Counter subscriptionBatchesSentTotal;
    public final Counter subscriptionErrorsTotal;

    public final Histogram ingestorFetchDurationSeconds;
    public final Histogram cacheWaitDurationSeconds;
    public final Histogram archiveUploadDurationSeconds;
    public final Histogram archiveColdReadDurationSeconds;
    public final Histogram subscriptionBackpressureSeconds;

    public Metrics(CollectorRegistry registry) {
        this.ingestorHeadBlock = Gauge.build()
            .name("flight_ingestor_head_block")
            .help("Latest chain head observed via WSS newHeads")
            .register(registry);

        this.ingestorCommittedBlock = Gauge.build()
            .name("flight_ingestor_committed_block")
            .help("Last contiguously-committed block number")
            .register(registry);

        this.ingestorPendingDepth = Gauge.build()
            .name("flight_ingestor_pending_depth")
            .help("Number of completed fetches awaiting in-order commit")
            .register(registry);

        this.cachePruneFloor = Gauge.build()
            .name("flight_cache_prune_floor")
            .help("Exclusive floor: blocks with number < this are pruned from hot cache")
            .register(registry);

        this.subscriptionsActive = Gauge.build()
            .name("flight_subscriptions_active")
            .labelNames("dataset")
            .help("Currently active client subscriptions")
            .register(registry);

        this.ingestorBlocksCommittedTotal = Counter.build()
            .name("flight_ingestor_blocks_committed_total")
            .help("Blocks successfully committed to the cache")
            .register(registry);

        this.ingestorFetchRetriesTotal = Counter.build()
            .name("flight_ingestor_fetch_retries_total")
            .help("Block-fetch retry attempts due to RPC errors")
            .register(registry);

        this.ingestorCommitErrorsTotal = Counter.build()
            .name("flight_ingestor_commit_errors_total")
            .help("Commit-path failures (serialization or RocksDB write)")
            .register(registry);

        this.cacheReadsTotal = Counter.build()
            .name("flight_cache_reads_total")
            .labelNames("dataset", "result")
            .help("Cache reads labelled by dataset and result (loaded|waited|pruned)")
            .register(registry);

        this.archiveUploadsTotal = Counter.build()
            .name("flight_archive_uploads_total")
            .labelNames("status")
            .help("S3 archive uploads (status=success|failure)")
            .register(registry);

        this.archiveColdReadsTotal = Counter.build()
            .name("flight_archive_cold_reads_total")
            .labelNames("dataset", "status")
            .help("Cold-tier reads from S3 archive (status=hit|miss|failure)")
            .register(registry);

        this.subscriptionsTotal = Counter.build()
            .name("flight_subscriptions_total")
            .labelNames("dataset")
            .help("Subscriptions created since startup")
            .register(registry);

        this.subscriptionBatchesSentTotal = Counter.build()
            .name("flight_subscription_batches_sent_total")
            .labelNames("dataset")
            .help("Record batches pushed to clients via putNext")
            .register(registry);

        this.subscriptionErrorsTotal = Counter.build()
            .name("flight_subscription_errors_total")
            .labelNames("dataset")
            .help("Subscription error terminations")
            .register(registry);

        this.ingestorFetchDurationSeconds = Histogram.build()
            .name("flight_ingestor_fetch_duration_seconds")
            .help("Full-block fetch latency (successful attempts only)")
            .register(registry);

        this.cacheWaitDurationSeconds = Histogram.build()
            .name("flight_cache_wait_duration_seconds")
            .labelNames("dataset")
            .help("Time a reader blocked in getOrWait before data became available")
            .register(registry);

        this.archiveUploadDurationSeconds = Histogram.build()
            .name("flight_archive_upload_duration_seconds")
            .buckets(0.1, 0.5, 1, 5, 10, 30, 60, 120)
            .help("Time to archive+upload a chunk to S3")
            .register(registry);

        this.archiveColdReadDurationSeconds = Histogram.build()
            .name("flight_archive_cold_read_duration_seconds")
            .help("Time to fetch a single block from the S3 cold tier")
            .register(registry);

        this.subscriptionBackpressureSeconds = Histogram.build()
            .name("flight_subscription_backpressure_seconds")
            .labelNames("dataset")
            .buckets(
                0.0001, 0.001, 0.01, 0.05,
                0.1, 0.25, 0.5, 1.0,
                2.5, 5.0, 10.0, 30.0, 60.0
            )
            .help(
                "Time the subscription's producer thread parked in awaitReady "
                + "before the gRPC stream had send window again. Observed once "
                + "per putNext (0 on the fast path); high tail = slow consumer."
            )
            .register(registry);
    }

    /** Production factory: registers against {@link CollectorRegistry#defaultRegistry}. */
    public static Metrics forDefaultRegistry() {
        return new Metrics(CollectorRegistry.defaultRegistry);
    }

    /** Test factory: isolated registry, safe to construct repeatedly. */
    public static Metrics forTesting() {
        return new Metrics(new CollectorRegistry());
    }
}
