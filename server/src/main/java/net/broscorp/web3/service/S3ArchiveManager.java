package net.broscorp.web3.service;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
import net.broscorp.web3.archive.StreamingChunkWriter;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3-backed cold archive.
 *
 * <p>Object layout: {@code <dataset>/<startBlock>_<endBlock>.arrow} (exclusive
 * end) holds an Arrow IPC stream with one {@code RecordBatch} per block in
 * ascending block-number order. Chunk sizes need not be uniform across S3 —
 * legacy or differently-configured runs may have written chunks at other
 * sizes, and the reader-side index treats whatever ranges it finds as
 * authoritative.
 *
 * <p>Both archive writes and cold-tier reads are serialized via a
 * single-threaded executor so that ingestion-driven archives can never
 * overlap (which would race with pruning).
 *
 * <p>Available chunks are discovered via a periodic {@code ListObjectsV2}
 * sweep into an in-memory index per dataset: a sorted map from
 * {@code chunkStart} to {@code chunkEnd}, plus the largest chunk length
 * seen. Lookups are by block number: walk the map backward from
 * {@link NavigableMap#floorEntry} (bounded by
 * {@code blockNumber - maxChunkLength}, since chunks starting earlier
 * than that cannot reach the block) and pick the covering chunk with
 * the largest end so a sequential reader spans as much forward range as
 * possible. The index is authoritative: a miss skips the GET entirely.
 * The index is rebuilt at startup, on a fixed schedule, and once after
 * every successful {@link #archiveRange} so locally-written chunks are
 * visible immediately.
 */
@Slf4j
public class S3ArchiveManager implements ArchiveManager {

    private final S3Client s3;
    private final String bucket;
    private final String keyPrefix;
    private final BlockchainCache cache;
    private final Converter converter;
    private final BufferAllocator allocator;
    private final Metrics metrics;
    private final ExecutorService executor =
        Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "s3-archive");
            t.setDaemon(true);
            return t;
        });
    private final ScheduledExecutorService indexRefresher =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "s3-archive-index");
            t.setDaemon(true);
            return t;
        });
    private volatile Map<String, DatasetIndex> chunkIndex = Map.of();

    private record DatasetIndex(NavigableMap<Long, Long> chunks, long maxChunkLength) {
        static final DatasetIndex EMPTY = new DatasetIndex(new TreeMap<>(), 0L);
    }

    public S3ArchiveManager(
        S3Client s3,
        String bucket,
        BlockchainCache cache,
        Converter converter,
        BufferAllocator allocator,
        Metrics metrics,
        Duration indexRefreshInterval
    ) {
        this.s3 = s3;
        int slash = bucket.indexOf('/');
        if (slash < 0) {
            this.bucket = bucket;
            this.keyPrefix = "";
        } else {
            this.bucket = bucket.substring(0, slash);
            String prefix = bucket.substring(slash + 1);
            this.keyPrefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
        }
        this.cache = cache;
        this.converter = converter;
        this.allocator = allocator;
        this.metrics = metrics;

        refreshIndex();
        long intervalMs = indexRefreshInterval.toMillis();
        indexRefresher.scheduleWithFixedDelay(
            this::refreshIndexQuietly,
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public CompletableFuture<Void> archiveRange(
        long startBlock,
        long endBlock
    ) {
        return CompletableFuture.runAsync(
            () -> {
                log.info("Archiving range [{}, {}) to S3", startBlock, endBlock);
                Histogram.Timer timer =
                    metrics.archiveUploadDurationSeconds.startTimer();
                try {
                    archiveDataset(
                        DATASET_BLOCKS,
                        converter.getBlockSchema(),
                        startBlock,
                        endBlock,
                        cache::getBlock
                    );
                    archiveDataset(
                        DATASET_LOGS,
                        converter.getLogSchema(),
                        startBlock,
                        endBlock,
                        cache::getLogs
                    );
                    metrics.archiveUploadsTotal.labels("success").inc();
                    log.info("Archive complete for [{}, {})", startBlock, endBlock);
                    indexRefresher.execute(this::refreshIndexQuietly);
                } catch (Exception e) {
                    metrics.archiveUploadsTotal.labels("failure").inc();
                    throw new RuntimeException(
                        "archiveRange [" + startBlock + ", " + endBlock + ") failed",
                        e
                    );
                } finally {
                    timer.observeDuration();
                }
            },
            executor
        );
    }

    @Override
    public CompletableFuture<ChunkReader> openChunkReader(
        String dataset,
        long blockNumber
    ) {
        DatasetIndex idx = chunkIndex.getOrDefault(dataset, DatasetIndex.EMPTY);
        Map.Entry<Long, Long> covering = findCoveringChunk(idx, blockNumber);
        if (covering == null) {
            metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
            return CompletableFuture.completedFuture(null);
        }
        long chunkStart = covering.getKey();
        long chunkEnd = covering.getValue();
        return CompletableFuture.supplyAsync(
            () -> {
                Histogram.Timer timer =
                    metrics.archiveColdReadDurationSeconds.startTimer();
                String key = ArchiveKey.objectKey(keyPrefix, dataset, chunkStart, chunkEnd);
                Path tmp = Path.of(
                    System.getProperty("java.io.tmpdir"),
                    "cold-" + dataset + "-" + UUID.randomUUID() + ".arrow"
                );
                try {
                    try {
                        s3.getObject(
                            GetObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build(),
                            tmp
                        );
                    } catch (NoSuchKeyException e) {
                        metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
                        log.debug("Archive object {} not found", key);
                        deleteQuietly(tmp);
                        return null;
                    }
                    return new S3ChunkReader(
                        allocator,
                        dataset,
                        chunkStart,
                        chunkEnd,
                        blockNumberFieldFor(dataset),
                        tmp,
                        metrics
                    );
                } catch (Exception e) {
                    deleteQuietly(tmp);
                    metrics.archiveColdReadsTotal.labels(dataset, "failure").inc();
                    throw new RuntimeException(
                        "Failed to open chunk reader for " + key, e
                    );
                } finally {
                    timer.observeDuration();
                }
            },
            executor
        );
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (Exception e) {
            log.warn("Failed to delete cold-read temp file {}", file, e);
        }
    }

    private static String blockNumberFieldFor(String dataset) {
        return switch (dataset) {
            case DATASET_BLOCKS -> Converter.BLOCK_NUMBER;
            case DATASET_LOGS -> Converter.LOG_BLOCK_NUMBER;
            default -> throw new IllegalArgumentException(
                "Unknown dataset: " + dataset
            );
        };
    }

    private interface CacheLookup {
        Optional<byte[]> apply(long blockNumber) throws Exception;
    }

    private void archiveDataset(
        String dataset,
        Schema schema,
        long startBlock,
        long endBlock,
        CacheLookup lookup
    ) throws Exception {
        Path tmp = Files.createTempFile("archive-" + dataset + "-", ".arrow");
        try {
            int batches;
            try (
                FileChannel ch = FileChannel.open(
                    tmp,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
                );
                StreamingChunkWriter w = new StreamingChunkWriter(allocator, schema, ch)
            ) {
                for (long n = startBlock; n < endBlock; n++) {
                    byte[] ipc = lookup.apply(n).orElse(null);
                    if (ipc == null) {
                        log.warn("Chunk skip: no {} for block {}", dataset, n);
                        continue;
                    }
                    w.appendBatch(ipc);
                }
                batches = w.batchesWritten();
            }

            if (batches == 0) {
                log.warn(
                    "Archive skipped for {} [{}, {}): nothing cached",
                    dataset,
                    startBlock,
                    endBlock
                );
                return;
            }

            String key = ArchiveKey.objectKey(keyPrefix, dataset, startBlock, endBlock);
            s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(tmp)
            );
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private void refreshIndexQuietly() {
        try {
            refreshIndex();
        } catch (Exception e) {
            log.warn("Failed to refresh S3 archive index — keeping previous", e);
        }
    }

    private void refreshIndex() {
        Map<String, DatasetIndex> next = new HashMap<>();
        next.put(DATASET_BLOCKS, listChunks(DATASET_BLOCKS));
        next.put(DATASET_LOGS, listChunks(DATASET_LOGS));
        chunkIndex = Map.copyOf(next);
        log.info(
            "Refreshed S3 archive index: blocks={} chunks (maxLen={}), "
                + "logs={} chunks (maxLen={})",
            chunkIndex.get(DATASET_BLOCKS).chunks().size(),
            chunkIndex.get(DATASET_BLOCKS).maxChunkLength(),
            chunkIndex.get(DATASET_LOGS).chunks().size(),
            chunkIndex.get(DATASET_LOGS).maxChunkLength()
        );
    }

    private DatasetIndex listChunks(String dataset) {
        String prefix = keyPrefix + dataset + "/";
        NavigableMap<Long, Long> chunks = new TreeMap<>();
        long maxLen = 0L;
        String token = null;
        do {
            ListObjectsV2Response resp = s3.listObjectsV2(
                ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .continuationToken(token)
                    .build()
            );
            for (var obj : resp.contents()) {
                long[] range = parseChunkRange(obj.key(), prefix);
                if (range == null) {
                    log.warn("Skipping unrecognised archive key: {}", obj.key());
                    continue;
                }
                // Two list entries with the same start (e.g. <start>_<a>.arrow
                // and <start>_<b>.arrow co-existing after a chunk-size change)
                // collapse to the wider end so the index keeps the most
                // coverage from a single map slot. Different-start overlaps
                // are kept as separate entries; lookup picks among them.
                chunks.merge(range[0], range[1], Math::max);
                maxLen = Math.max(maxLen, range[1] - range[0]);
            }
            token = Boolean.TRUE.equals(resp.isTruncated())
                ? resp.nextContinuationToken()
                : null;
        } while (token != null);
        return new DatasetIndex(chunks, maxLen);
    }

    /**
     * Returns the index entry for the chunk covering {@code blockNumber}, or
     * {@code null} if none does. When multiple chunks cover the block, picks
     * the one with the largest {@code end} so a sequential reader spans the
     * widest forward range.
     */
    private static Map.Entry<Long, Long> findCoveringChunk(
        DatasetIndex idx,
        long blockNumber
    ) {
        if (idx.chunks().isEmpty()) return null;
        long lowerStartBound = blockNumber - idx.maxChunkLength() + 1;
        Map.Entry<Long, Long> best = null;
        for (var entry :
            idx.chunks().headMap(blockNumber, true).descendingMap().entrySet()) {
            if (entry.getKey() < lowerStartBound) break;
            if (entry.getValue() > blockNumber
                && (best == null || entry.getValue() > best.getValue())) {
                best = entry;
            }
        }
        return best;
    }

    private static long[] parseChunkRange(String key, String prefix) {
        if (!key.startsWith(prefix)) return null;
        String suffix = key.substring(prefix.length());
        if (!suffix.endsWith(".arrow")) return null;
        String stem = suffix.substring(0, suffix.length() - ".arrow".length());
        int underscore = stem.indexOf('_');
        if (underscore <= 0 || underscore == stem.length() - 1) return null;
        try {
            long start = Long.parseLong(stem.substring(0, underscore));
            long end = Long.parseLong(stem.substring(underscore + 1));
            if (start < 0 || end <= start) return null;
            return new long[] {start, end};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public void close() {
        indexRefresher.shutdown();
        executor.shutdown();
    }
}
