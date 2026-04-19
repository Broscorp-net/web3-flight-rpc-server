package net.broscorp.web3.converter;

import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

@Slf4j
public final class Converter {

    public static final String LOG_ADDRESS = "address";
    public static final String LOG_DATA = "data";
    public static final String LOG_TOPICS = "topics";
    public static final String LOG_TOPIC_CHILD_NAME = "topic";
    public static final String LOG_BLOCK_NUMBER = "blockNumber";
    public static final String LOG_TRANSACTION_HASH = "transactionHash";
    public static final String LOG_BLOCK_HASH = "blockHash";
    public static final String LOG_INDEX = "logIndex";
    public static final String LOG_REMOVED = "removed";
    public static final String LOG_TIMESTAMP = "timestamp";
    public static final String LOG_TX_STATUS = "transactionStatus";

    private static final Schema LOG_SCHEMA = new Schema(
        List.of(
            Field.nullable(LOG_ADDRESS, new ArrowType.Utf8()),
            Field.nullable(LOG_DATA, new ArrowType.Utf8()),
            new Field(
                LOG_TOPICS,
                new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(
                    Field.nullable(LOG_TOPIC_CHILD_NAME, new ArrowType.Utf8())
                )
            ),
            Field.nullable(LOG_BLOCK_NUMBER, new ArrowType.Int(64, true)),
            Field.nullable(LOG_TRANSACTION_HASH, new ArrowType.Utf8()),
            Field.nullable(LOG_BLOCK_HASH, new ArrowType.Utf8()),
            Field.nullable(LOG_INDEX, new ArrowType.Int(32, true)),
            Field.nullable(LOG_REMOVED, new ArrowType.Bool()),
            Field.nullable(LOG_TIMESTAMP, new ArrowType.Int(64, true)),
            Field.nullable(LOG_TX_STATUS, new ArrowType.Int(32, true))
        )
    );

    public static final String BLOCK_NUMBER = "number";
    public static final String BLOCK_HASH = "hash";
    public static final String BLOCK_PARENT_HASH = "parentHash";
    public static final String BLOCK_TIMESTAMP = "timestamp";
    public static final String BLOCK_MINER = "miner";
    public static final String BLOCK_GAS_LIMIT = "gasLimit";
    public static final String BLOCK_GAS_USED = "gasUsed";
    public static final String BLOCK_SIZE = "size";
    public static final String BLOCK_EXTRA_DATA = "extraData";
    public static final String BLOCK_TRANSACTIONS = "transactions";
    public static final String BLOCK_TX_CHILD_NAME = "txHash";

    private static final Schema BLOCK_SCHEMA = new Schema(
        List.of(
            Field.nullable(BLOCK_NUMBER, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_HASH, new ArrowType.Utf8()),
            Field.nullable(BLOCK_PARENT_HASH, new ArrowType.Utf8()),
            Field.nullable(BLOCK_TIMESTAMP, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_MINER, new ArrowType.Utf8()),
            Field.nullable(BLOCK_GAS_LIMIT, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_GAS_USED, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_SIZE, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_EXTRA_DATA, new ArrowType.Utf8()),
            new Field(
                BLOCK_TRANSACTIONS,
                new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(
                    Field.nullable(BLOCK_TX_CHILD_NAME, new ArrowType.Utf8())
                )
            )
        )
    );

    public Schema getLogSchema() {
        return LOG_SCHEMA;
    }

    public Schema getBlockSchema() {
        return BLOCK_SCHEMA;
    }

    public byte[] toLogIpcBytes(
        BufferAllocator allocator,
        long blockNumber,
        String blockHash,
        long timestamp,
        List<Log> logs,
        Map<String, TransactionReceipt> receipts
    ) {
        try (
            VectorSchemaRoot root = VectorSchemaRoot.create(
                LOG_SCHEMA,
                allocator
            )
        ) {
            final VarCharVector addressVector = (VarCharVector) root.getVector(
                LOG_ADDRESS
            );
            final VarCharVector dataVector = (VarCharVector) root.getVector(
                LOG_DATA
            );
            final ListVector topicsVector = (ListVector) root.getVector(
                LOG_TOPICS
            );
            final BigIntVector blockNumberVector =
                (BigIntVector) root.getVector(LOG_BLOCK_NUMBER);
            final VarCharVector transactionHashVector =
                (VarCharVector) root.getVector(LOG_TRANSACTION_HASH);
            final VarCharVector blockHashVector =
                (VarCharVector) root.getVector(LOG_BLOCK_HASH);
            final IntVector logIndexVector = (IntVector) root.getVector(
                LOG_INDEX
            );
            final BitVector removedVector = (BitVector) root.getVector(
                LOG_REMOVED
            );
            final BigIntVector timestampVector = (BigIntVector) root.getVector(
                LOG_TIMESTAMP
            );
            final IntVector txStatusVector = (IntVector) root.getVector(
                LOG_TX_STATUS
            );

            int rowCount = logs.isEmpty() ? 1 : logs.size();
            root.allocateNew();

            if (logs.isEmpty()) {
                // Sentinel Row
                blockNumberVector.setSafe(0, blockNumber);
                setNullableString(blockHashVector, 0, blockHash);
                timestampVector.setSafe(0, timestamp);
            } else {
                final UnionListWriter topicsWriter = topicsVector.getWriter();
                for (int i = 0; i < logs.size(); i++) {
                    Log logEntry = logs.get(i);
                    setNullableString(addressVector, i, logEntry.getAddress());
                    setNullableString(dataVector, i, logEntry.getData());
                    setNullableString(
                        transactionHashVector,
                        i,
                        logEntry.getTransactionHash()
                    );
                    setNullableString(blockHashVector, i, blockHash);
                    blockNumberVector.setSafe(i, blockNumber);
                    logIndexVector.setSafe(
                        i,
                        logEntry.getLogIndex().intValue()
                    );
                    removedVector.setSafe(i, logEntry.isRemoved() ? 1 : 0);
                    timestampVector.setSafe(i, timestamp);

                    TransactionReceipt receipt = receipts.get(
                        logEntry.getTransactionHash()
                    );
                    if (receipt != null) {
                        txStatusVector.setSafe(
                            i,
                            "0x1".equals(receipt.getStatus()) ? 1 : 0
                        );
                    } else {
                        txStatusVector.setNull(i);
                    }

                    List<String> topics = logEntry.getTopics();
                    topicsWriter.setPosition(i);
                    topicsWriter.startList();
                    for (String topic : topics) {
                        topicsWriter.writeVarChar(topic);
                    }
                    topicsWriter.endList();
                }
            }
            root.setRowCount(rowCount);
            return rootToIpcBytes(root);
        }
    }

    public byte[] toBlockIpcBytes(
        BufferAllocator allocator,
        EthBlock.Block block
    ) {
        try (
            VectorSchemaRoot root = VectorSchemaRoot.create(
                BLOCK_SCHEMA,
                allocator
            )
        ) {
            final BigIntVector numberVector = (BigIntVector) root.getVector(
                BLOCK_NUMBER
            );
            final VarCharVector hashVector = (VarCharVector) root.getVector(
                BLOCK_HASH
            );
            final VarCharVector parentHashVector =
                (VarCharVector) root.getVector(BLOCK_PARENT_HASH);
            final BigIntVector timestampVector = (BigIntVector) root.getVector(
                BLOCK_TIMESTAMP
            );
            final VarCharVector minerVector = (VarCharVector) root.getVector(
                BLOCK_MINER
            );
            final BigIntVector gasLimitVector = (BigIntVector) root.getVector(
                BLOCK_GAS_LIMIT
            );
            final BigIntVector gasUsedVector = (BigIntVector) root.getVector(
                BLOCK_GAS_USED
            );
            final BigIntVector sizeVector = (BigIntVector) root.getVector(
                BLOCK_SIZE
            );
            final VarCharVector extraDataVector =
                (VarCharVector) root.getVector(BLOCK_EXTRA_DATA);
            final ListVector transactionsVector = (ListVector) root.getVector(
                BLOCK_TRANSACTIONS
            );

            root.allocateNew();
            numberVector.setSafe(0, block.getNumber().longValue());
            setNullableString(hashVector, 0, block.getHash());
            setNullableString(parentHashVector, 0, block.getParentHash());
            timestampVector.setSafe(0, block.getTimestamp().longValue());
            setNullableString(minerVector, 0, block.getMiner());
            gasLimitVector.setSafe(0, block.getGasLimit().longValue());
            gasUsedVector.setSafe(0, block.getGasUsed().longValue());
            sizeVector.setSafe(0, block.getSize().longValue());
            setNullableString(extraDataVector, 0, block.getExtraData());

            final UnionListWriter txWriter = transactionsVector.getWriter();
            txWriter.setPosition(0);
            txWriter.startList();
            for (EthBlock.TransactionResult tx : block.getTransactions()) {
                txWriter.writeVarChar((String) tx.get());
            }
            txWriter.endList();

            root.setRowCount(1);
            return rootToIpcBytes(root);
        }
    }

    private byte[] rootToIpcBytes(VectorSchemaRoot root) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (
            ArrowStreamWriter writer = new ArrowStreamWriter(
                root,
                null,
                Channels.newChannel(out)
            )
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Arrow root", e);
        }
        return out.toByteArray();
    }

    private static void setNullableString(
        VarCharVector vector,
        int index,
        String value
    ) {
        if (value != null) {
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vector.setNull(index);
        }
    }
}
