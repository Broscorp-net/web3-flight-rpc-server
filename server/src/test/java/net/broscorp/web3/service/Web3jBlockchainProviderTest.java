package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import net.broscorp.web3.service.Web3jBlockchainProvider.EthBlockReceipts;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthLog;

class Web3jBlockchainProviderTest {

    @Test
    void blockJsonRpcErrorBecomesJsonRpcException() {
        EthBlock blockRes = new EthBlock();
        blockRes.setError(new Response.Error(-32005, "limit exceeded"));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.ONE, blockRes, okLogs(), okReceipts(0)
            )
        )
            .isInstanceOfSatisfying(JsonRpcException.class, e -> {
                assertThat(e.method()).isEqualTo("eth_getBlockByNumber");
                assertThat(e.code()).isEqualTo(-32005);
            });
    }

    @Test
    void logsJsonRpcErrorBecomesJsonRpcException() {
        EthLog logsRes = new EthLog();
        logsRes.setError(new Response.Error(429, "Too Many Requests"));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.ONE, okBlock(0), logsRes, okReceipts(0)
            )
        )
            .isInstanceOfSatisfying(JsonRpcException.class, e -> {
                assertThat(e.method()).isEqualTo("eth_getLogs");
                assertThat(e.code()).isEqualTo(429);
            });
    }

    @Test
    void receiptsJsonRpcErrorBecomesJsonRpcException() {
        EthBlockReceipts receiptsRes = new EthBlockReceipts();
        receiptsRes.setError(new Response.Error(-32603, "internal error"));

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.ONE, okBlock(0), okLogs(), receiptsRes
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
                BigInteger.valueOf(42L), blockRes, okLogs(), okReceipts(0)
            )
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("eth_getBlockByNumber for block 42");
    }

    @Test
    void nullLogsResultWithoutErrorThrowsIllegalState() {
        EthLog logsRes = new EthLog(); // no setResult

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), okBlock(0), logsRes, okReceipts(0)
            )
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("eth_getLogs for block 42");
    }

    @Test
    void nullReceiptsResultWithoutErrorThrowsIllegalState() {
        EthBlockReceipts receiptsRes = new EthBlockReceipts(); // no setResult

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), okBlock(0), okLogs(), receiptsRes
            )
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("eth_getBlockReceipts for block 42");
    }

    @Test
    void receiptsCountMismatchThrowsIllegalState() {
        // Block declares 3 transactions, receipts only carry 1 — exact shape
        // produced by upstream nodes that drop the receipts payload silently.
        EthBlock blockRes = okBlock(3);

        assertThatThrownBy(() ->
            Web3jBlockchainProvider.assembleFullBlockData(
                BigInteger.valueOf(42L), blockRes, okLogs(), okReceipts(1)
            )
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("receipts/transactions mismatch")
            .hasMessageContaining("3 txs")
            .hasMessageContaining("1 receipts");
    }

    @Test
    void happyPathReturnsFullBlockDataWithMatchingReceipts() {
        EthBlock blockRes = okBlock(2);

        FullBlockData data = Web3jBlockchainProvider.assembleFullBlockData(
            BigInteger.valueOf(7L), blockRes, okLogs(), okReceipts(2)
        );

        assertThat(data.block()).isSameAs(blockRes.getBlock());
        assertThat(data.logs()).isEmpty();
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

    private static EthLog okLogs() {
        EthLog res = new EthLog();
        res.setResult(Collections.emptyList());
        return res;
    }

    private static EthBlockReceipts okReceipts(int count) {
        EthBlockReceipts res = new EthBlockReceipts();
        List<ExtendedTransactionReceipt> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ExtendedTransactionReceipt r = new ExtendedTransactionReceipt();
            r.setTransactionHash("0xtx" + i);
            list.add(r);
        }
        res.setResult(list);
        return res;
    }
}
