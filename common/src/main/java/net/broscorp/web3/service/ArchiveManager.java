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
     * Returns the Arrow IPC bytes for a single block's entry in the named
     * dataset, or {@code null} if the covering archive object does not exist
     * or the block is absent from it.
     */
    CompletableFuture<byte[]> getFromArchive(String dataset, long blockNumber);
}
