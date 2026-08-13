package net.broscorp.web3.archive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Appends single-batch Arrow IPC payloads to a multi-batch chunk stream
 * one at a time, without buffering prior batches in memory.
 *
 * <p>Underlying writer holds a single reusable {@link VectorSchemaRoot};
 * each {@link #appendBatch} call decodes the input bytes, transfers the
 * vectors into the root, writes the batch to the sink channel, and clears
 * the root for the next call. Suitable for streaming chunk assembly to a
 * file or network channel where heap pressure must stay bounded.
 *
 * <p>Input batches are always read uncompressed (they come from the hot
 * cache / converter). Output batches are compressed per
 * {@link ChunkCompression}, which is recorded in each batch's metadata so
 * readers decompress transparently.
 */
public final class StreamingChunkWriter implements AutoCloseable {

    private final BufferAllocator allocator;
    private final VectorSchemaRoot target;
    private final ArrowStreamWriter writer;
    private int batches;

    /** Writes an uncompressed chunk stream. */
    public StreamingChunkWriter(
        BufferAllocator allocator,
        Schema schema,
        WritableByteChannel sink
    ) throws IOException {
        this(allocator, schema, sink, ChunkCompression.NONE);
    }

    public StreamingChunkWriter(
        BufferAllocator allocator,
        Schema schema,
        WritableByteChannel sink,
        ChunkCompression compression
    ) throws IOException {
        this.allocator = allocator;
        this.target = VectorSchemaRoot.create(schema, allocator);
        this.writer = compression.enabled()
            ? new ArrowStreamWriter(
                target,
                null,
                sink,
                IpcOption.DEFAULT,
                compression.writerFactory(),
                compression.codec(),
                compression.level()
            )
            : new ArrowStreamWriter(target, null, sink);
        this.writer.start();
    }

    public void appendBatch(byte[] singleBatchIpc) throws IOException {
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(singleBatchIpc),
                allocator
            )
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            int rows = src.getRowCount();
            target.allocateNew();
            for (int field = 0; field < src.getFieldVectors().size(); field++) {
                TransferPair tp = src
                    .getVector(field)
                    .makeTransferPair(target.getVector(field));
                for (int row = 0; row < rows; row++) {
                    tp.copyValueSafe(row, row);
                }
            }
            target.setRowCount(rows);
            writer.writeBatch();
            target.clear();
            batches++;
        }
    }

    public int batchesWritten() {
        return batches;
    }

    @Override
    public void close() throws IOException {
        try {
            writer.end();
        } finally {
            try {
                writer.close();
            } finally {
                target.close();
            }
        }
    }
}
