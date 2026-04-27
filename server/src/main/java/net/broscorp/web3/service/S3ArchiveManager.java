package net.broscorp.web3.service;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3-backed cold archive.
 *
 * <p>Object layout: {@code <dataset>/<startBlock>_<endBlock>.arrow} (exclusive
 * end) holds an Arrow IPC stream with one {@code RecordBatch} per block in
 * ascending block-number order. Readers walk the stream and match each
 * batch's block-number column against the requested block, so chunks
 * tolerate gaps and non-aligned starts (legacy data from older versions of
 * the sweep).
 *
 * <p>Both archive writes and cold-tier reads are serialized via a
 * single-threaded executor so that ingestion-driven archives can never
 * overlap (which would race with pruning).
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

    public S3ArchiveManager(
        S3Client s3,
        String bucket,
        BlockchainCache cache,
        Converter converter,
        BufferAllocator allocator,
        Metrics metrics
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
        long chunkStart
    ) {
        return CompletableFuture.supplyAsync(
            () -> {
                Histogram.Timer timer =
                    metrics.archiveColdReadDurationSeconds.startTimer();
                long chunkEnd = chunkStart + ArchiveKey.CHUNK_SIZE;
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

    public void close() {
        executor.shutdown();
    }
}
