package net.broscorp.web3.service;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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
                Path tmp;
                try {
                    tmp = Files.createTempFile("cold-" + dataset + "-", ".arrow");
                } catch (Exception e) {
                    timer.observeDuration();
                    metrics.archiveColdReadsTotal.labels(dataset, "failure").inc();
                    throw new RuntimeException(
                        "Failed to create cold-read temp file for " + key, e
                    );
                }
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
                    return new S3ChunkReader(allocator, dataset, chunkStart, tmp, metrics);
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

    /**
     * Forward-only cursor over a single chunk file. State machine:
     * {@code nextBatchIndex} is the index of the batch that the next
     * {@link ArrowStreamReader#loadNextBatch} call would load. {@link #readBlock}
     * is required to be called with strictly increasing block numbers; the
     * reader advances the underlying stream until the requested batch is loaded
     * and then returns it serialized as single-batch IPC bytes.
     */
    private static class S3ChunkReader implements ChunkReader {

        private final String dataset;
        private final long chunkStart;
        private final long chunkEnd;
        private final Path file;
        private final InputStream in;
        private final ArrowStreamReader reader;
        private final Metrics metrics;
        private long nextBatchIndex;

        S3ChunkReader(
            BufferAllocator allocator,
            String dataset,
            long chunkStart,
            Path file,
            Metrics metrics
        ) throws Exception {
            this.dataset = dataset;
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkStart + ArchiveKey.CHUNK_SIZE;
            this.file = file;
            this.metrics = metrics;
            this.in = Files.newInputStream(file);
            try {
                this.reader = new ArrowStreamReader(in, allocator);
            } catch (Exception e) {
                in.close();
                throw e;
            }
        }

        @Override
        public long chunkStart() {
            return chunkStart;
        }

        @Override
        public long chunkEnd() {
            return chunkEnd;
        }

        @Override
        public byte[] readBlock(long blockNumber) throws Exception {
            if (blockNumber < chunkStart || blockNumber >= chunkEnd) {
                throw new IllegalArgumentException(
                    "Block " + blockNumber + " is outside chunk ["
                        + chunkStart + ", " + chunkEnd + ")"
                );
            }
            long targetIndex = blockNumber - chunkStart;
            if (targetIndex < nextBatchIndex) {
                throw new IllegalArgumentException(
                    "Out-of-order chunk read: blockNumber=" + blockNumber
                        + " is before the cursor for chunk starting at " + chunkStart
                );
            }
            while (nextBatchIndex <= targetIndex) {
                if (!reader.loadNextBatch()) {
                    metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
                    return null;
                }
                nextBatchIndex++;
            }
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (
                ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(out)
                )
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            metrics.archiveColdReadsTotal.labels(dataset, "hit").inc();
            return out.toByteArray();
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (Exception e) {
                log.warn("Failed to close ArrowStreamReader for {}", file, e);
            }
            try {
                in.close();
            } catch (Exception e) {
                log.warn("Failed to close InputStream for {}", file, e);
            }
            deleteQuietly(file);
        }
    }
}
