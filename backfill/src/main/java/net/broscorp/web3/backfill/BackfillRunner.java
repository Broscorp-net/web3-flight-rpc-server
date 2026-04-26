package net.broscorp.web3.backfill;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
import net.broscorp.web3.archive.StreamingChunkWriter;
import net.broscorp.web3.backfill.ratelimit.TokenBucket;
import net.broscorp.web3.backfill.sink.S3ChunkWriter;
import net.broscorp.web3.backfill.source.BlockSource;
import net.broscorp.web3.backfill.source.RpcBlockSource;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Batch backfill: fetches a historical block range and writes Arrow IPC chunks
 * to S3 using the same key layout the live archive sweep uses.
 *
 * <p>Designed to run as a one-shot Kubernetes Job per chain. Idempotent —
 * existing chunks are skipped, so re-runs over the same range are cheap.
 *
 * <p>Range alignment: chunks are 1000-block aligned starting at 0. The job
 * processes whole chunks fully contained in {@code [from, to]}; partial
 * chunks at either edge are left for the live ingestor to fill.
 *
 * <p>Memory: per-chunk RAM is bounded by {@code BACKFILL_FETCH_PARALLELISM}
 * — at most that many {@link FullBlockData} objects are alive at once.
 * Chunk bytes are streamed block-by-block to two temp files (one per dataset)
 * and uploaded directly from disk; nothing buffers an entire chunk on heap.
 */
@Slf4j
public final class BackfillRunner {

    private static final long HEARTBEAT_INTERVAL_NS = 30_000_000_000L;

    private record ChunkStats(
        long blocksWritten,
        long blocksBytes,
        long logsBytes,
        long elapsedNs
    ) {}

    private BackfillRunner() {}

    public static void main(String[] args) throws Exception {
        long fromBlock = requiredLongEnv("BACKFILL_FROM_BLOCK");
        long toBlock = requiredLongEnv("BACKFILL_TO_BLOCK");
        String sourceKind = envOr("BACKFILL_SOURCE", "rpc");
        int fetchParallelism = (int) longEnvOr("BACKFILL_FETCH_PARALLELISM", 8L);
        boolean skipExisting = boolEnvOr("BACKFILL_SKIP_EXISTING", true);
        double maxRps = doubleEnvOr("BACKFILL_MAX_RPS", 0.0);

        String s3Bucket = requiredEnv("S3_BUCKET");
        String s3Region = envOr("S3_REGION", "us-east-1");
        String awsAccessKey = requiredEnv("AWS_ACCESS_KEY");
        String awsSecretKey = requiredEnv("AWS_SECRET_KEY");

        long firstChunkStart = ArchiveKey.chunkStartFor(fromBlock);
        long lastChunkStartExclusive = ArchiveKey.chunkStartFor(toBlock);
        if (firstChunkStart >= lastChunkStartExclusive) {
            log.warn(
                "Range [{}, {}] yields no whole chunks — nothing to do",
                fromBlock,
                toBlock
            );
            return;
        }

        long chunkCount =
            (lastChunkStartExclusive - firstChunkStart) / ArchiveKey.CHUNK_SIZE;
        log.info(
            "Backfill plan: {} chunks of {} blocks each — [{}, {}) using source={}, fetchParallelism={}, maxRps={}",
            chunkCount,
            ArchiveKey.CHUNK_SIZE,
            firstChunkStart,
            lastChunkStartExclusive,
            sourceKind,
            fetchParallelism,
            maxRps > 0 ? Double.toString(maxRps) : "unlimited"
        );

        TokenBucket rateLimiter = maxRps > 0
            ? new TokenBucket(maxRps, Math.max(maxRps, 3.0))
            : null;
        Converter converter = new Converter();
        try (
            BlockSource source = buildSource(sourceKind, rateLimiter);
            S3Client s3 = S3Client.builder()
                .region(Region.of(s3Region))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsAccessKey, awsSecretKey)
                    )
                )
                .build();
            S3ChunkWriter writer = new S3ChunkWriter(s3, s3Bucket);
            BufferAllocator allocator = new RootAllocator()
        ) {
            long runStartNs = System.nanoTime();
            long processed = 0;
            long uploaded = 0;
            long skippedExisting = 0;
            long aborted = 0;
            long totalBlocks = 0;
            long totalBytes = 0;
            for (
                long chunkStart = firstChunkStart;
                chunkStart < lastChunkStartExclusive;
                chunkStart += ArchiveKey.CHUNK_SIZE
            ) {
                long chunkEnd = chunkStart + ArchiveKey.CHUNK_SIZE;
                if (
                    skipExisting &&
                    writer.chunkExists(ArchiveManager.DATASET_BLOCKS, chunkStart, chunkEnd) &&
                    writer.chunkExists(ArchiveManager.DATASET_LOGS, chunkStart, chunkEnd)
                ) {
                    log.info(
                        "Chunk [{}, {}) already in S3 — skipping",
                        chunkStart,
                        chunkEnd
                    );
                    skippedExisting++;
                } else {
                    ChunkStats stats = processChunk(
                        chunkStart,
                        chunkEnd,
                        source,
                        writer,
                        converter,
                        allocator,
                        fetchParallelism
                    );
                    if (stats != null) {
                        uploaded++;
                        totalBlocks += stats.blocksWritten();
                        totalBytes += stats.blocksBytes() + stats.logsBytes();
                    } else {
                        aborted++;
                    }
                }
                processed++;
                if (processed % 10 == 0 || processed == chunkCount) {
                    long runElapsedSec = Math.max(
                        1L, (System.nanoTime() - runStartNs) / 1_000_000_000L
                    );
                    double bps = (double) totalBlocks / runElapsedSec;
                    long remaining = chunkCount - processed;
                    String eta = (bps > 0 && remaining > 0)
                        ? formatDuration((long) (remaining * ArchiveKey.CHUNK_SIZE / bps))
                        : "—";
                    log.info(
                        "Progress: {}/{} chunks (uploaded={}, skipped={}, aborted={}) — {} blocks @ {} blocks/sec, {} written, ETA {}",
                        processed,
                        chunkCount,
                        uploaded,
                        skippedExisting,
                        aborted,
                        totalBlocks,
                        String.format("%.1f", bps),
                        humanBytes(totalBytes),
                        eta
                    );
                }
            }
            long totalElapsedSec = (System.nanoTime() - runStartNs) / 1_000_000_000L;
            log.info(
                "Backfill complete in {}: {}/{} chunks (uploaded={}, skipped={}, aborted={}), {} blocks, {} written",
                formatDuration(totalElapsedSec),
                processed,
                chunkCount,
                uploaded,
                skippedExisting,
                aborted,
                totalBlocks,
                humanBytes(totalBytes)
            );
        }
    }

    /**
     * Streams one chunk through to S3 with bounded memory.
     *
     * <p>Producer maintains a sliding window of up to {@code fetchParallelism}
     * outstanding {@link CompletableFuture}s. Consumer drains in block-number
     * order: as each future resolves, the block is converted to per-block
     * Arrow IPC bytes and appended to two open {@link StreamingChunkWriter}s
     * (blocks + logs) backed by temp files. The {@link FullBlockData}
     * reference is dropped immediately after, so heap usage is bounded by the
     * window size, not the chunk size.
     */
    private static ChunkStats processChunk(
        long chunkStart,
        long chunkEnd,
        BlockSource source,
        S3ChunkWriter writer,
        Converter converter,
        BufferAllocator allocator,
        int fetchParallelism
    ) throws Exception {
        long t0 = System.nanoTime();
        Map<Long, CompletableFuture<FullBlockData>> pending = new HashMap<>();
        long fetchCursor = chunkStart;
        for (
            int i = 0;
            i < fetchParallelism && fetchCursor < chunkEnd;
            i++, fetchCursor++
        ) {
            pending.put(fetchCursor, source.fetchBlock(fetchCursor));
        }

        Path blocksTmp = Files.createTempFile("backfill-blocks-", ".arrow");
        Path logsTmp = Files.createTempFile("backfill-logs-", ".arrow");
        ChunkStats result = null;
        try {
            int blocksWritten = 0;
            int blocksBatches;
            int logsBatches;
            try (
                FileChannel blocksCh = FileChannel.open(
                    blocksTmp,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
                );
                FileChannel logsCh = FileChannel.open(
                    logsTmp,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
                );
                StreamingChunkWriter blocksWriter = new StreamingChunkWriter(
                    allocator, converter.getBlockSchema(), blocksCh
                );
                StreamingChunkWriter logsWriter = new StreamingChunkWriter(
                    allocator, converter.getLogSchema(), logsCh
                )
            ) {
                long lastHeartbeatNs = t0;
                int span = (int) (chunkEnd - chunkStart);
                for (long n = chunkStart; n < chunkEnd; n++) {
                    CompletableFuture<FullBlockData> f = pending.remove(n);
                    FullBlockData data;
                    try {
                        data = awaitWithHeartbeat(
                            f, n, chunkStart, chunkEnd, blocksWritten, span, t0
                        );
                    } catch (ExecutionException ee) {
                        log.error(
                            "Chunk [{}, {}) aborted: fetch failed for block {}: {}",
                            chunkStart,
                            chunkEnd,
                            n,
                            ee.getCause() == null ? ee.toString() : ee.getCause().toString()
                        );
                        return null;
                    }
                    if (data == null) {
                        log.warn(
                            "Chunk [{}, {}) aborted: block {} returned null",
                            chunkStart,
                            chunkEnd,
                            n
                        );
                        return null;
                    }
                    blocksWriter.appendBatch(
                        converter.toBlockIpcBytes(allocator, data.block())
                    );
                    logsWriter.appendBatch(
                        converter.toLogIpcBytes(
                            allocator,
                            data.block().getNumber().longValue(),
                            data.block().getHash(),
                            data.block().getTimestamp().longValue(),
                            data.logs(),
                            data.receipts()
                        )
                    );
                    blocksWritten++;
                    if (fetchCursor < chunkEnd) {
                        pending.put(fetchCursor, source.fetchBlock(fetchCursor));
                        fetchCursor++;
                    }
                    long now = System.nanoTime();
                    if (
                        now - lastHeartbeatNs >= HEARTBEAT_INTERVAL_NS
                        && blocksWritten < span
                    ) {
                        logHeartbeat(
                            "fetched", chunkStart, chunkEnd, blocksWritten, span, t0
                        );
                        lastHeartbeatNs = now;
                    }
                }
                blocksBatches = blocksWriter.batchesWritten();
                logsBatches = logsWriter.batchesWritten();
            }

            if (blocksBatches == 0 || logsBatches == 0) {
                log.warn(
                    "Chunk [{}, {}) yielded no data after assembly — skipping upload",
                    chunkStart,
                    chunkEnd
                );
                return null;
            }

            writer.writeChunkFromFile(
                ArchiveManager.DATASET_BLOCKS, chunkStart, chunkEnd, blocksTmp
            );
            writer.writeChunkFromFile(
                ArchiveManager.DATASET_LOGS, chunkStart, chunkEnd, logsTmp
            );
            long elapsedNs = System.nanoTime() - t0;
            long blocksSize = Files.size(blocksTmp);
            long logsSize = Files.size(logsTmp);
            double bps = elapsedNs > 0
                ? (double) blocksWritten * 1_000_000_000.0 / elapsedNs
                : 0;
            log.info(
                "Chunk [{}, {}) uploaded in {}ms ({} blocks/sec, blocks={} logs={})",
                chunkStart,
                chunkEnd,
                elapsedNs / 1_000_000L,
                String.format("%.1f", bps),
                humanBytes(blocksSize),
                humanBytes(logsSize)
            );
            result = new ChunkStats(blocksWritten, blocksSize, logsSize, elapsedNs);
            return result;
        } finally {
            for (CompletableFuture<FullBlockData> f : pending.values()) {
                f.cancel(true);
            }
            Files.deleteIfExists(blocksTmp);
            Files.deleteIfExists(logsTmp);
            if (result == null) {
                log.debug(
                    "Chunk [{}, {}) cleanup: temp files removed, no upload",
                    chunkStart,
                    chunkEnd
                );
            }
        }
    }

    private static FullBlockData awaitWithHeartbeat(
        CompletableFuture<FullBlockData> f,
        long blockNumber,
        long chunkStart,
        long chunkEnd,
        int blocksWritten,
        int span,
        long chunkStartedNs
    ) throws InterruptedException, ExecutionException {
        while (true) {
            try {
                return f.get(HEARTBEAT_INTERVAL_NS, TimeUnit.NANOSECONDS);
            } catch (TimeoutException te) {
                long elapsedSec =
                    (System.nanoTime() - chunkStartedNs) / 1_000_000_000L;
                log.info(
                    "Chunk [{}, {}): waiting on block {} ({}/{} written, {}s elapsed)",
                    chunkStart,
                    chunkEnd,
                    blockNumber,
                    blocksWritten,
                    span,
                    elapsedSec
                );
            }
        }
    }

    private static void logHeartbeat(
        String phase,
        long chunkStart,
        long chunkEnd,
        int blocksWritten,
        int span,
        long chunkStartedNs
    ) {
        long elapsedSec =
            (System.nanoTime() - chunkStartedNs) / 1_000_000_000L;
        double bps = elapsedSec > 0 ? (double) blocksWritten / elapsedSec : 0;
        log.info(
            "Chunk [{}, {}) {}: {}/{} blocks ({}s elapsed, {} blocks/sec)",
            chunkStart,
            chunkEnd,
            phase,
            blocksWritten,
            span,
            elapsedSec,
            String.format("%.1f", bps)
        );
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.1f KiB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format("%.1f MiB", mb);
        return String.format("%.2f GiB", mb / 1024.0);
    }

    private static String formatDuration(long seconds) {
        if (seconds <= 0) return "0s";
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        if (h > 0) return String.format("%dh%02dm", h, m);
        if (m > 0) return String.format("%dm%02ds", m, s);
        return String.format("%ds", s);
    }

    private static BlockSource buildSource(String kind, TokenBucket rateLimiter) {
        return switch (kind.toLowerCase()) {
            case "rpc" -> new RpcBlockSource(
                requiredEnv("HTTP_NODE_URL"), rateLimiter
            );
            case "bigquery" -> throw new UnsupportedOperationException(
                "BACKFILL_SOURCE=bigquery not yet implemented"
            );
            default -> throw new IllegalArgumentException(
                "Unknown BACKFILL_SOURCE: " + kind + " (expected rpc|bigquery)"
            );
        };
    }

    private static String requiredEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return v;
    }

    private static long requiredLongEnv(String name) {
        return Long.parseLong(requiredEnv(name));
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    private static long longEnvOr(String name, long fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : Long.parseLong(v);
    }

    private static double doubleEnvOr(String name, double fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : Double.parseDouble(v);
    }

    private static boolean boolEnvOr(String name, boolean fallback) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) return fallback;
        return switch (v.trim().toLowerCase()) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException(
                "Invalid bool for " + name + ": " + v
            );
        };
    }
}
