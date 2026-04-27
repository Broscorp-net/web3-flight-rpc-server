package net.broscorp.web3.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.metrics.Metrics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * Forward-only cursor over a single archive chunk file.
 *
 * <p>Block lookup is done by <b>identity</b> (matching the block-number
 * column on row 0 of each loaded batch), not by positional index. That makes
 * the reader robust to chunks containing gaps (missing blocks) or chunks
 * whose key does not start on a {@code CHUNK_SIZE} boundary, both of which
 * exist in legacy data written by older versions of the archive sweep.
 *
 * <p>Contract: {@link #readBlock} must be called with strictly increasing
 * block numbers. A null return means the requested block is not in the
 * stream (either skipped past it on the way to a later batch, or hit
 * end-of-stream); subsequent calls with higher block numbers may still
 * succeed.
 */
@Slf4j
class S3ChunkReader implements ArchiveManager.ChunkReader {

    private final String dataset;
    private final long chunkStart;
    private final long chunkEnd;
    private final String blockNumberField;
    private final Path file;
    private final InputStream in;
    private final ArrowStreamReader reader;
    private final Metrics metrics;
    private long lastReturnedBlock;
    private long currentBatchBlock;
    private boolean currentBatchLoaded;
    private boolean exhausted;

    S3ChunkReader(
        BufferAllocator allocator,
        String dataset,
        long chunkStart,
        long chunkEnd,
        String blockNumberField,
        Path file,
        Metrics metrics
    ) throws Exception {
        this.dataset = dataset;
        this.chunkStart = chunkStart;
        this.chunkEnd = chunkEnd;
        this.blockNumberField = blockNumberField;
        this.file = file;
        this.metrics = metrics;
        this.lastReturnedBlock = chunkStart - 1;
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
        if (blockNumber <= lastReturnedBlock) {
            throw new IllegalArgumentException(
                "Out-of-order chunk read: blockNumber=" + blockNumber
                    + " is not strictly greater than last returned "
                    + lastReturnedBlock + " in chunk starting at " + chunkStart
            );
        }

        while (true) {
            if (!currentBatchLoaded) {
                if (exhausted || !reader.loadNextBatch()) {
                    exhausted = true;
                    metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
                    return null;
                }
                currentBatchBlock = readBatchBlockNumber();
                currentBatchLoaded = true;
            }

            if (currentBatchBlock == blockNumber) {
                byte[] data = serializeCurrentBatch();
                lastReturnedBlock = blockNumber;
                currentBatchLoaded = false;
                metrics.archiveColdReadsTotal.labels(dataset, "hit").inc();
                return data;
            }
            if (currentBatchBlock > blockNumber) {
                // Requested block is missing from the chunk. Keep the loaded
                // batch in case a later call with a higher number lands on it.
                metrics.archiveColdReadsTotal.labels(dataset, "miss").inc();
                return null;
            }
            // currentBatchBlock < blockNumber: advance.
            currentBatchLoaded = false;
        }
    }

    private long readBatchBlockNumber() throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        BigIntVector v = (BigIntVector) root.getVector(blockNumberField);
        if (v == null) {
            throw new IllegalStateException(
                "Chunk file " + file + " has no field " + blockNumberField
            );
        }
        if (v.getValueCount() == 0) {
            throw new IllegalStateException(
                "Empty batch in chunk file " + file
            );
        }
        if (v.isNull(0)) {
            throw new IllegalStateException(
                "Null block number on row 0 of batch in chunk file " + file
            );
        }
        return v.get(0);
    }

    private byte[] serializeCurrentBatch() throws Exception {
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
        try {
            Files.deleteIfExists(file);
        } catch (Exception e) {
            log.warn("Failed to delete cold-read temp file {}", file, e);
        }
    }
}
