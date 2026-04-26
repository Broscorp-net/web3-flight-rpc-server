package net.broscorp.web3.backfill.source;

import java.util.concurrent.CompletableFuture;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;

/**
 * Source for {@link FullBlockData} during a batch backfill.
 *
 * <p>Implementations must be safe to call concurrently from many fetch
 * workers. Concurrency control (parallelism, rate limiting) is the source's
 * responsibility; the orchestrator simply pipes block numbers in.
 */
public interface BlockSource extends AutoCloseable {

    /**
     * Returns the full data for one block. Failures are surfaced via
     * {@link CompletableFuture#exceptionally}; the orchestrator decides
     * whether to retry, skip, or fail the whole chunk.
     */
    CompletableFuture<FullBlockData> fetchBlock(long blockNumber);

    @Override
    void close();
}
