package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * Interface for fetching raw blockchain data.
 */
public interface BlockchainProvider {
    record FullBlockData(
        EthBlock.Block block,
        List<Log> logs,
        Map<String, TransactionReceipt> receipts
    ) {}

    /**
     * Fetches the latest block number.
     */
    CompletableFuture<BigInteger> getLatestBlockNumber();

    /**
     * Fetches a full block with logs and receipts.
     */
    CompletableFuture<FullBlockData> fetchFullBlock(BigInteger blockNumber);
}
