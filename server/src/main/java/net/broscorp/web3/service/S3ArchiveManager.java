package net.broscorp.web3.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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
 * end) holds an Arrow IPC stream with exactly one {@code RecordBatch} per block
 * in order. Blocks are addressable in O(1) via
 * {@code batchIndex = blockNumber - startBlock}.
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
    public CompletableFuture<byte[]> getFromArchive(
        String dataset,
        long blockNumber
    ) {
        return CompletableFuture.supplyAsync(
            () -> {
                Histogram.Timer timer =
                    metrics.archiveColdReadDurationSeconds.startTimer();
                long chunkStart = ArchiveKey.chunkStartFor(blockNumber);
                long chunkEnd = chunkStart + ArchiveKey.CHUNK_SIZE;
                String key = ArchiveKey.objectKey(keyPrefix, dataset, chunkStart, chunkEnd);
                try {
                    byte[] object;
                    try {
                        object = s3
                            .getObjectAsBytes(
                                GetObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build()
                            )
                            .asByteArray();
                    } catch (NoSuchKeyException e) {
                        metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
                        log.debug("Archive object {} not found", key);
                        return null;
                    }
                    byte[] extracted = extractBatch(object, blockNumber - chunkStart);
                    String status = extracted == null ? "miss" : "hit";
                    metrics.archiveColdReadsTotal.labels(dataset, status).inc();
                    return extracted;
                } catch (Exception e) {
                    metrics.archiveColdReadsTotal.labels(dataset, "failure").inc();
                    throw new RuntimeException(
                        "Failed to extract block " + blockNumber + " from " + key,
                        e
                    );
                } finally {
                    timer.observeDuration();
                }
            },
            executor
        );
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

    private byte[] extractBatch(byte[] streamBytes, long batchIndex)
        throws Exception {
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(streamBytes),
                allocator
            )
        ) {
            for (long i = 0; i <= batchIndex; i++) {
                if (!reader.loadNextBatch()) {
                    return null;
                }
            }
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (
                ArrowStreamWriter writer = new ArrowStreamWriter(
                    root,
                    null,
                    Channels.newChannel(out)
                )
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
    }

    public void close() {
        executor.shutdown();
    }
}
