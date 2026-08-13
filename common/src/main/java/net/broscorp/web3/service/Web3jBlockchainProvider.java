package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

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

        CompletableFuture<EthBlockReceipts> receiptsFuture =
            ethGetBlockReceipts(blockNumber).sendAsync();

        return CompletableFuture.allOf(blockFuture, receiptsFuture)
            .thenApply(v ->
                assembleFullBlockData(
                    blockNumber,
                    blockFuture.join(),
                    receiptsFuture.join()
                )
            );
    }

    /**
     * Validates the two RPC responses for a block fetch and assembles them
     * into a {@link FullBlockData}. Fails loudly on JSON-RPC errors,
     * null-result-without-error, and receipts/transactions count mismatches —
     * see {@code BackfillRunner} bug investigation 2026-04-28 for context.
     *
     * <p>Logs come from the receipts rather than a separate {@code eth_getLogs}
     * call: since we always fetch exactly one block, every log of that block is
     * already carried by {@code eth_getBlockReceipts}, so a dedicated log query
     * would re-fetch rows we hold. See {@link #extractLogs}.
     */
    static FullBlockData assembleFullBlockData(
        BigInteger blockNumber,
        EthBlock blockRes,
        EthBlockReceipts receiptsRes
    ) {
        checkRpcError("eth_getBlockByNumber", blockRes);
        checkRpcError("eth_getBlockReceipts", receiptsRes);

        EthBlock.Block block = blockRes.getBlock();
        if (block == null) {
            throw new MalformedRpcResponseException(
                "eth_getBlockByNumber for block " + blockNumber
                    + " returned no result and no error"
            );
        }

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

        List<Log> logs = extractLogs(blockNumber, receiptList);

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

    /**
     * Flattens the per-transaction log arrays of {@code eth_getBlockReceipts}
     * into the block-wide, {@code logIndex}-ordered list {@code eth_getLogs}
     * used to return.
     *
     * <p>Receipts arrive in transaction order and logs are ordered within a
     * receipt, so the concatenation is already sorted; the explicit sort only
     * guards against an upstream node returning receipts out of order, which the
     * transaction-hash-keyed receipts map would not otherwise catch.
     *
     * <p>Missing {@code logs} or {@code logIndex} is treated as a malformed
     * response rather than an empty block: both are always present on a complete
     * receipt, and {@link MalformedRpcResponseException} is classified transient
     * so the caller retries instead of committing a block with dropped logs.
     */
    private static List<Log> extractLogs(
        BigInteger blockNumber,
        List<ExtendedTransactionReceipt> receiptList
    ) {
        List<Log> logs = new ArrayList<>();
        for (ExtendedTransactionReceipt receipt : receiptList) {
            List<Log> receiptLogs = receipt.getLogs();
            if (receiptLogs == null) {
                throw new MalformedRpcResponseException(
                    "eth_getBlockReceipts for block " + blockNumber
                        + " returned receipt " + receipt.getTransactionHash()
                        + " with no logs array "
                        + "(likely partial/error response from upstream node)"
                );
            }
            for (Log entry : receiptLogs) {
                if (entry.getLogIndexRaw() == null) {
                    throw new MalformedRpcResponseException(
                        "eth_getBlockReceipts for block " + blockNumber
                            + " returned a log without logIndex on receipt "
                            + receipt.getTransactionHash()
                            + " (likely partial/error response from upstream node)"
                    );
                }
                // Nodes that omit transactionHash on a receipt's nested logs
                // would break the per-log receipt join in Converter; the owning
                // receipt supplies it here so the join cannot silently miss.
                if (entry.getTransactionHash() == null) {
                    entry.setTransactionHash(receipt.getTransactionHash());
                }
                logs.add(entry);
            }
        }
        logs.sort(Comparator.comparing(Log::getLogIndex));
        return List.copyOf(logs);
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
