package net.broscorp.web3.service;

import java.util.concurrent.CompletableFuture;

/**
 * Cold-tier storage for pruned blocks.
 *
 * <p>Datasets mirror the cache: {@code "blocks"} (per-block headers) and
 * {@code "logs"} (per-block logs + sentinel).
 */
public interface ArchiveManager {
    String DATASET_BLOCKS = "blocks";
    String DATASET_LOGS = "logs";

    /**
     * Bundles cached IPC bytes for the half-open range {@code [startBlock, endBlock)}
     * for both datasets and uploads them to cold storage.
     */
    CompletableFuture<Void> archiveRange(long startBlock, long endBlock);

    /**
     * Opens a forward-only sequential reader for the archive chunk that
     * contains {@code blockNumber} in the named dataset. The chunk's actual
     * range is exposed via {@link ChunkReader#chunkStart()} /
     * {@link ChunkReader#chunkEnd()} — it may differ from the current
     * archive-write chunk size when legacy or differently-sized chunks
     * coexist in storage. Resolves to {@code null} if no chunk covers the
     * block. Callers MUST close the returned reader to release the temp
     * file backing it.
     */
    CompletableFuture<ChunkReader> openChunkReader(String dataset, long blockNumber);

    /**
     * Forward-only cursor over a single archive chunk. {@link #readBlock} must
     * be called with strictly increasing block numbers; out-of-order reads
     * throw. Backed by a temp file released on {@link #close}.
     */
    interface ChunkReader extends AutoCloseable {
        long chunkStart();

        long chunkEnd();

        /**
         * Returns the single-batch Arrow IPC bytes for {@code blockNumber}, or
         * {@code null} if the block is absent from the chunk (e.g. the chunk
         * was assembled with gaps).
         */
        byte[] readBlock(long blockNumber) throws Exception;

        @Override
        void close();
    }
}
