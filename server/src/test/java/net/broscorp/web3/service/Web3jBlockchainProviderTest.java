package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import net.broscorp.web3.service.Web3jBlockchainProvider.EthBlockReceipts;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

class Web3jBlockchainProviderTest {

    @Test
    void blockJsonRpcErrorBecomesJsonRpcException() {
        EthBlock blockRes = new EthBlock();
        blockRes.setError(new Response.Error(-32005, "limit exceeded"));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.ONE, blockRes, okReceipts(0)
            )
        )
            .isInstanceOfSatisfying(JsonRpcException.class, e -> {
                assertThat(e.method()).isEqualTo("eth_getBlockByNumber");
                assertThat(e.code()).isEqualTo(-32005);
            });
    }

    @Test
    void receiptsJsonRpcErrorBecomesJsonRpcException() {
        EthBlockReceipts receiptsRes = new EthBlockReceipts();
        receiptsRes.setError(new Response.Error(-32603, "internal error"));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.ONE, okBlock(0), receiptsRes
            )
        )
            .isInstanceOfSatisfying(JsonRpcException.class, e -> {
                assertThat(e.method()).isEqualTo("eth_getBlockReceipts");
                assertThat(e.code()).isEqualTo(-32603);
            });
    }

    @Test
    void nullBlockResultWithoutErrorThrowsIllegalState() {
        EthBlock blockRes = new EthBlock();
        // No setResult, no setError — exact silent-corruption shape we hit in prod.

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), blockRes, okReceipts(0)
            )
        )
            .isInstanceOf(MalformedRpcResponseException.class)
            .hasMessageContaining("eth_getBlockByNumber for block 42");
    }

    @Test
    void nullReceiptsResultWithoutErrorThrowsIllegalState() {
        EthBlockReceipts receiptsRes = new EthBlockReceipts(); // no setResult

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), okBlock(0), receiptsRes
            )
        )
            .isInstanceOf(MalformedRpcResponseException.class)
            .hasMessageContaining("eth_getBlockReceipts for block 42");
    }

    @Test
    void receiptsCountMismatchThrowsIllegalState() {
        // Block declares 3 transactions, receipts only carry 1 — exact shape
        // produced by upstream nodes that drop the receipts payload silently.
        EthBlock blockRes = okBlock(3);

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), blockRes, okReceipts(1)
            )
        )
            .isInstanceOf(MalformedRpcResponseException.class)
            .hasMessageContaining("receipts/transactions mismatch")
            .hasMessageContaining("3 txs")
            .hasMessageContaining("1 receipts");
    }

    @Test
    void receiptWithoutLogsArrayThrowsMalformed() {
        // logs == null is a truncated receipt, not a transaction that emitted
        // nothing — that case carries an empty array.
        EthBlockReceipts receiptsRes = receipts(receipt("0xtx0", null));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), okBlock(1), receiptsRes
            )
        )
            .isInstanceOf(MalformedRpcResponseException.class)
            .hasMessageContaining("with no logs array")
            .hasMessageContaining("0xtx0");
    }

    @Test
    void logWithoutLogIndexThrowsMalformed() {
        Log noIndex = log(null, "0xtx0");

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L),
                okBlock(1),
                receipts(receipt("0xtx0", List.of(noIndex)))
            )
        )
            .isInstanceOf(MalformedRpcResponseException.class)
            .hasMessageContaining("without logIndex")
            .hasMessageContaining("0xtx0");
    }

    @Test
    void logsAreFlattenedFromReceiptsInLogIndexOrder() {
        // Receipts deliberately out of transaction order: the flattened result
        // must still be ordered by logIndex, the way eth_getLogs returned it.
        EthBlockReceipts receiptsRes = receipts(
            receipt("0xtx1", List.of(log("0x2", "0xtx1"))),
            receipt("0xtx0", List.of(log("0x0", "0xtx0"), log("0x1", "0xtx0")))
        );

        FullBlockData data = Web3jBlockchainProvider.assembleFullBlockData(
            BigInteger.valueOf(7L), okBlock(2), receiptsRes
        );

        assertThat(data.logs()).hasSize(3);
        assertThat(data.logs())
            .extracting(l -> l.getLogIndex().intValue())
            .containsExactly(0, 1, 2);
        assertThat(data.logs())
            .extracting(Log::getTransactionHash)
            .containsExactly("0xtx0", "0xtx0", "0xtx1");
    }

    @Test
    void logsInheritTransactionHashFromOwningReceiptWhenAbsent() {
        // Some nodes omit transactionHash on a receipt's nested logs; without
        // backfilling it the per-log receipt join in Converter would miss.
        EthBlockReceipts receiptsRes = receipts(
            receipt("0xtx0", List.of(log("0x0", null)))
        );

        FullBlockData data = Web3jBlockchainProvider.assembleFullBlockData(
            BigInteger.valueOf(7L), okBlock(1), receiptsRes
        );

        assertThat(data.logs())
            .singleElement()
            .extracting(Log::getTransactionHash)
            .isEqualTo("0xtx0");
    }

    @Test
    void blockWithNoLogsYieldsEmptyLogList() {
        FullBlockData data = Web3jBlockchainProvider.assembleFullBlockData(
            BigInteger.valueOf(7L),
            okBlock(1),
            receipts(receipt("0xtx0", List.of()))
        );

        assertThat(data.logs()).isEmpty();
        assertThat(data.receipts()).hasSize(1);
    }

    @Test
    void happyPathReturnsFullBlockDataWithMatchingReceipts() {
        EthBlock blockRes = okBlock(2);

        FullBlockData data = Web3jBlockchainProvider.assembleFullBlockData(
            BigInteger.valueOf(7L), blockRes, okReceipts(2)
        );

        assertThat(data.block()).isSameAs(blockRes.getBlock());
        assertThat(data.receipts()).hasSize(2);
        assertThat(data.receipts().keySet())
            .containsExactlyInAnyOrder("0xtx0", "0xtx1");
    }

    private static EthBlock okBlock(int txCount) {
        EthBlock res = new EthBlock();
        EthBlock.Block block = new EthBlock.Block();
        List<EthBlock.TransactionResult> txs = new ArrayList<>();
        for (int i = 0; i < txCount; i++) {
            txs.add(new EthBlock.TransactionHash("0xtx" + i));
        }
        block.setTransactions(txs);
        res.setResult(block);
        return res;
    }

    /** {@code count} receipts, each carrying one log at logIndex {@code i}. */
    private static EthBlockReceipts okReceipts(int count) {
        List<ExtendedTransactionReceipt> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String txHash = "0xtx" + i;
            list.add(
                receipt(txHash, List.of(log("0x" + Integer.toHexString(i), txHash)))
            );
        }
        return receipts(list.toArray(new ExtendedTransactionReceipt[0]));
    }

    private static EthBlockReceipts receipts(
        ExtendedTransactionReceipt... receipts
    ) {
        EthBlockReceipts res = new EthBlockReceipts();
        res.setResult(Arrays.asList(receipts));
        return res;
    }

    private static ExtendedTransactionReceipt receipt(
        String txHash,
        List<Log> logs
    ) {
        ExtendedTransactionReceipt r = new ExtendedTransactionReceipt();
        r.setTransactionHash(txHash);
        r.setLogs(logs);
        return r;
    }

    private static Log log(String logIndexHex, String txHash) {
        Log l = new Log();
        l.setLogIndex(logIndexHex);
        l.setTransactionHash(txHash);
        return l;
    }
}
