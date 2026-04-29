package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.Web3jService;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthLog;

@Slf4j
public class Web3jBlockchainProvider implements BlockchainProvider {

    private final Web3j web3j;
    private final Web3jService web3jService;

    public Web3jBlockchainProvider(Web3j web3j, Web3jService web3jService) {
        this.web3j = web3j;
        this.web3jService = web3jService;
    }

    @Override
    public CompletableFuture<BigInteger> getLatestBlockNumber() {
        return web3j
            .ethBlockNumber()
            .sendAsync()
            .thenApply(res -> {
                checkRpcError("eth_blockNumber", res);
                return res.getBlockNumber();
            });
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

        CompletableFuture<EthBlockReceipts> receiptsFuture =
            ethGetBlockReceipts(blockNumber).sendAsync();

        return CompletableFuture.allOf(blockFuture, logsFuture, receiptsFuture)
            .thenApply(v ->
                assembleFullBlockData(
                    blockNumber,
                    blockFuture.join(),
                    logsFuture.join(),
                    receiptsFuture.join()
                )
            );
    }

    /**
     * Validates the three RPC responses for a block fetch and assembles them
     * into a {@link FullBlockData}. Fails loudly on JSON-RPC errors,
     * null-result-without-error, and receipts/transactions count mismatches —
     * see {@code BackfillRunner} bug investigation 2026-04-28 for context.
     */
    static FullBlockData assembleFullBlockData(
        BigInteger blockNumber,
        EthBlock blockRes,
        EthLog logsRes,
        EthBlockReceipts receiptsRes
    ) {
        checkRpcError("eth_getBlockByNumber", blockRes);
        checkRpcError("eth_getLogs", logsRes);
        checkRpcError("eth_getBlockReceipts", receiptsRes);

        EthBlock.Block block = blockRes.getBlock();
        if (block == null) {
            throw new MalformedRpcResponseException(
                "eth_getBlockByNumber for block " + blockNumber
                    + " returned no result and no error"
            );
        }

        if (logsRes.getLogs() == null) {
            throw new MalformedRpcResponseException(
                "eth_getLogs for block " + blockNumber
                    + " returned no result and no error"
            );
        }
        List<org.web3j.protocol.core.methods.response.Log> logs = logsRes
            .getLogs()
            .stream()
            .map(l -> (org.web3j.protocol.core.methods.response.Log) l.get())
            .toList();

        List<ExtendedTransactionReceipt> receiptList = receiptsRes
            .getBlockReceipts();
        if (receiptList == null) {
            throw new MalformedRpcResponseException(
                "eth_getBlockReceipts for block " + blockNumber
                    + " returned no result and no error"
            );
        }

        int txCount = block.getTransactions().size();
        if (receiptList.size() != txCount) {
            throw new MalformedRpcResponseException(
                "Block " + blockNumber + " receipts/transactions mismatch: "
                    + "block has " + txCount + " txs but received "
                    + receiptList.size() + " receipts "
                    + "(likely partial/error response from upstream node)"
            );
        }

        Map<String, ExtendedTransactionReceipt> receipts = receiptList
            .stream()
            .collect(
                Collectors.toMap(
                    ExtendedTransactionReceipt::getTransactionHash,
                    r -> r
                )
            );

        return new FullBlockData(block, logs, receipts);
    }

    private static void checkRpcError(String method, Response<?> response) {
        if (response.hasError()) {
            Response.Error err = response.getError();
            throw new JsonRpcException(method, err.getCode(), err.getMessage());
        }
    }

    private Request<?, EthBlockReceipts> ethGetBlockReceipts(
        BigInteger blockNumber
    ) {
        return new Request<>(
            "eth_getBlockReceipts",
            List.of("0x" + blockNumber.toString(16)),
            web3jService,
            EthBlockReceipts.class
        );
    }

    public static class EthBlockReceipts
        extends Response<List<ExtendedTransactionReceipt>> {
        public List<ExtendedTransactionReceipt> getBlockReceipts() {
            return getResult();
        }
    }
}
