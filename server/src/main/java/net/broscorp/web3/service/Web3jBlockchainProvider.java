package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.BatchRequest;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

@Slf4j
public class Web3jBlockchainProvider implements BlockchainProvider {

    private final Web3j web3j;

    public Web3jBlockchainProvider(Web3j web3j) {
        this.web3j = web3j;
    }

    @Override
    public CompletableFuture<BigInteger> getLatestBlockNumber() {
        return web3j
            .ethBlockNumber()
            .sendAsync()
            .thenApply(res -> res.getBlockNumber());
    }

    @Override
    public CompletableFuture<FullBlockData> fetchFullBlock(
        BigInteger blockNumber
    ) {
        CompletableFuture<EthBlock> blockFuture = web3j
            .ethGetBlockByNumber(
                new DefaultBlockParameterNumber(blockNumber),
                false
            )
            .sendAsync();

        EthFilter filter = new EthFilter(
            new DefaultBlockParameterNumber(blockNumber),
            new DefaultBlockParameterNumber(blockNumber),
            Collections.emptyList()
        );
        CompletableFuture<EthLog> logsFuture = web3j
            .ethGetLogs(filter)
            .sendAsync();

        return CompletableFuture.allOf(blockFuture, logsFuture).thenCompose(
            v -> {
                EthBlock blockRes = blockFuture.join();
                EthLog logsRes = logsFuture.join();

                if (blockRes.getBlock() == null) {
                    return CompletableFuture.failedFuture(
                        new RuntimeException(
                            "Block " + blockNumber + " not found"
                        )
                    );
                }

                List<org.web3j.protocol.core.methods.response.Log> logs =
                    logsRes
                        .getLogs()
                        .stream()
                        .map(l ->
                            (org.web3j.protocol.core.methods.response.Log) l.get()
                        )
                        .toList();

                return fetchReceipts(blockRes.getBlock()).thenApply(receipts ->
                    new FullBlockData(blockRes.getBlock(), logs, receipts)
                );
            }
        );
    }

    private CompletableFuture<Map<String, TransactionReceipt>> fetchReceipts(
        EthBlock.Block block
    ) {
        List<String> txHashes = block
            .getTransactions()
            .stream()
            .map(tx -> (String) tx.get())
            .toList();

        if (txHashes.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        BatchRequest batch = web3j.newBatch();
        txHashes.forEach(hash ->
            batch.add(web3j.ethGetTransactionReceipt(hash))
        );

        return batch
            .sendAsync()
            .thenApply(batchResponse -> {
                List<TransactionReceipt> receipts = batchResponse
                    .getResponses()
                    .stream()
                    .map(res ->
                        (
                            (org.web3j.protocol.core.methods.response.EthGetTransactionReceipt) res
                        ).getTransactionReceipt()
                    )
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .toList();

                return receipts
                    .stream()
                    .collect(
                        Collectors.toMap(
                            TransactionReceipt::getTransactionHash,
                            r -> r
                        )
                    );
            });
    }
}
