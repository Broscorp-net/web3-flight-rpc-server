package net.broscorp.web3.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import net.broscorp.web3.service.ExtendedTransactionReceipt;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

class ConverterTest {

    private Converter converter;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        converter = new Converter();
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void toLogIpcBytes_withFullData_populatesAllFieldsCorrectly()
        throws IOException {
        // GIVEN
        Log log = createTestLog("0x111", 1L);
        ExtendedTransactionReceipt receipt = new ExtendedTransactionReceipt();
        receipt.setTransactionHash(log.getTransactionHash());
        receipt.setStatus("0x1"); // Success
        receipt.setGasUsed("0x" + BigInteger.valueOf(21000).toString(16));
        receipt.setEffectiveGasPrice(
            "0x" + BigInteger.valueOf(20_000_000_000L).toString(16)
        );

        // WHEN
        byte[] ipcBytes = converter.toLogIpcBytes(
            allocator,
            1L,
            "0xbkHash1",
            123456789L,
            List.of(log),
            Map.of(log.getTransactionHash(), receipt)
        );

        // THEN
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(ipcBytes),
                allocator
            )
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(getString(root, "address", 0)).isEqualTo("0x111");
            assertThat(getLong(root, "blockNumber", 0)).isEqualTo(1L);
            assertThat(getLong(root, "timestamp", 0)).isEqualTo(123456789L);
            assertThat(getInt(root, "transactionStatus", 0)).isEqualTo(1);
            assertThat(getLong(root, "transactionGasUsed", 0)).isEqualTo(21000L);
            assertThat(getLong(root, "transactionEffectiveGasPrice", 0))
                .isEqualTo(20_000_000_000L);

            List<Object> topics = (List<Object>) (
                (ListVector) root.getVector("topics")
            ).getObject(0);
            assertThat(topics)
                .hasSize(2)
                .extracting(Object::toString)
                .containsExactly("topicA", "topicB");
        }
    }

    @Test
    void toLogIpcBytes_withNoLogs_emitsSentinelRow() throws IOException {
        // WHEN
        byte[] ipcBytes = converter.toLogIpcBytes(
            allocator,
            1L,
            "0xbkHash1",
            123456789L,
            Collections.emptyList(),
            Collections.emptyMap()
        );

        // THEN
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(ipcBytes),
                allocator
            )
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("address").isNull(0)).isTrue();
            assertThat(getLong(root, "blockNumber", 0)).isEqualTo(1L);
            assertThat(getLong(root, "timestamp", 0)).isEqualTo(123456789L);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void toBlockIpcBytes_populatesHeaderCorrectly() throws IOException {
        // GIVEN
        EthBlock.Block block = createTestBlock("0xAAA");

        // WHEN
        byte[] ipcBytes = converter.toBlockIpcBytes(allocator, block);

        // THEN
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(ipcBytes),
                allocator
            )
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(getLong(root, "number", 0)).isEqualTo(100L);
            assertThat(getString(root, "hash", 0)).isEqualTo("0xAAA");
            assertThat(getLong(root, "timestamp", 0)).isEqualTo(1672531200L);

            List<Object> txs = (List<Object>) (
                (ListVector) root.getVector("transactions")
            ).getObject(0);
            assertThat(txs)
                .hasSize(2)
                .extracting(Object::toString)
                .containsExactly("tx1", "tx2");
        }
    }

    private Log createTestLog(String address, Long blockNumber) {
        Log log = new Log();
        log.setAddress(address);
        log.setBlockNumber(
            blockNumber != null
                ? "0x" + BigInteger.valueOf(blockNumber).toString(16)
                : null
        );
        log.setData("0xdata1");
        log.setTransactionHash("0xtxHash1");
        log.setBlockHash("0xbkHash1");
        log.setLogIndex("0x" + BigInteger.valueOf(3).toString(16));
        log.setTopics(List.of("topicA", "topicB"));
        return log;
    }

    private EthBlock.Block createTestBlock(String hash) {
        EthBlock.Block block = new EthBlock.Block();
        List<String> txHashes = List.of("tx1", "tx2");
        List<EthBlock.TransactionResult> txResults = txHashes
            .stream()
            .map(txHash -> {
                EthBlock.TransactionResult mockedResult = mock(
                    EthBlock.TransactionResult.class
                );
                when(mockedResult.get()).thenReturn(txHash);
                return mockedResult;
            })
            .toList();

        block.setNumber("0x" + BigInteger.valueOf(100).toString(16));
        block.setHash(hash);
        block.setParentHash("0xparent");
        block.setTimestamp("0x" + BigInteger.valueOf(1672531200L).toString(16));
        block.setMiner("0xMiner");
        block.setGasLimit("0x" + BigInteger.valueOf(30_000_000).toString(16));
        block.setGasUsed("0x" + BigInteger.valueOf(50_000).toString(16));
        block.setSize("0x" + BigInteger.valueOf(1024).toString(16));
        block.setExtraData("0xextra");
        block.setTransactions(txResults);
        return block;
    }

    private String getString(VectorSchemaRoot root, String name, int index) {
        VarCharVector vector = (VarCharVector) root.getVector(name);
        return vector.isNull(index)
            ? null
            : new String(vector.get(index), StandardCharsets.UTF_8);
    }

    private Long getLong(VectorSchemaRoot root, String name, int index) {
        BigIntVector vector = (BigIntVector) root.getVector(name);
        return vector.isNull(index) ? null : vector.get(index);
    }

    private Integer getInt(VectorSchemaRoot root, String name, int index) {
        IntVector vector = (IntVector) root.getVector(name);
        return vector.isNull(index) ? null : vector.get(index);
    }
}
