package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import net.broscorp.web3.archive.ArchiveKey;
import net.broscorp.web3.archive.StreamingChunkWriter;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.EthBlock;

class S3ChunkReaderTest {

    private BufferAllocator allocator;
    private Converter converter;
    private Metrics metrics;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        converter = new Converter();
        metrics = Metrics.forTesting();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void readsContiguousBlocksInOrder() throws Exception {
        long chunkStart = 24963000L;
        Path file = writeChunk(chunkStart, new long[] {
            chunkStart, chunkStart + 1, chunkStart + 2, chunkStart + 3
        });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertBlock(r, chunkStart);
            assertBlock(r, chunkStart + 1);
            assertBlock(r, chunkStart + 2);
            assertBlock(r, chunkStart + 3);
        }
    }

    @Test
    void skipsBlocksThatAreSkippedByCaller() throws Exception {
        long chunkStart = 24963000L;
        Path file = writeChunk(chunkStart, new long[] {
            chunkStart, chunkStart + 1, chunkStart + 2, chunkStart + 3
        });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertBlock(r, chunkStart);
            // Skip chunkStart+1; jump to chunkStart+2.
            assertBlock(r, chunkStart + 2);
            // chunkStart+1 is now behind us, must throw.
            assertThatThrownBy(() -> r.readBlock(chunkStart + 1))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void returnsNullForBlockMissingInChunk_thenServesLaterBlock() throws Exception {
        long chunkStart = 24963000L;
        // Chunk has blocks 0, 1, 3 — block 2 is missing.
        Path file = writeChunk(chunkStart, new long[] {
            chunkStart, chunkStart + 1, chunkStart + 3
        });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertBlock(r, chunkStart);
            assertBlock(r, chunkStart + 1);
            // Block 2 is missing — must return null, NOT silently return block 3's data.
            assertThat(r.readBlock(chunkStart + 2)).isNull();
            // Block 3 is still readable on the next call.
            assertBlock(r, chunkStart + 3);
        }
    }

    @Test
    void readsMisalignedChunkByIdentity() throws Exception {
        // Simulates a legacy misaligned chunk: key advertises [24964344, 24965344)
        // but the data inside is genuinely those blocks. Reader matches on
        // identity, not on positional index from chunkStart.
        long chunkStart = 24964344L;
        long firstBlock = chunkStart;
        Path file = writeChunk(chunkStart, new long[] {
            firstBlock, firstBlock + 1, firstBlock + 2
        });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertBlock(r, firstBlock);
            assertBlock(r, firstBlock + 1);
            assertBlock(r, firstBlock + 2);
        }
    }

    @Test
    void rejectsBlockOutsideChunkRange() throws Exception {
        long chunkStart = 24963000L;
        Path file = writeChunk(chunkStart, new long[] { chunkStart });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertThatThrownBy(() -> r.readBlock(chunkStart - 1))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> r.readBlock(chunkStart + ArchiveKey.CHUNK_SIZE))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsRereadOfReturnedBlock() throws Exception {
        long chunkStart = 24963000L;
        Path file = writeChunk(chunkStart, new long[] { chunkStart, chunkStart + 1 });
        try (
            S3ChunkReader r = openReader(chunkStart, file)
        ) {
            assertBlock(r, chunkStart);
            assertThatThrownBy(() -> r.readBlock(chunkStart))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void closeDeletesTempFile() throws Exception {
        long chunkStart = 24963000L;
        Path file = writeChunk(chunkStart, new long[] { chunkStart });
        S3ChunkReader r = openReader(chunkStart, file);
        assertThat(Files.exists(file)).isTrue();
        r.close();
        assertThat(Files.exists(file)).isFalse();
    }

    private S3ChunkReader openReader(long chunkStart, Path file) throws Exception {
        return new S3ChunkReader(
            allocator,
            ArchiveManager.DATASET_BLOCKS,
            chunkStart,
            Converter.BLOCK_NUMBER,
            file,
            metrics
        );
    }

    private void assertBlock(S3ChunkReader r, long blockNumber) throws Exception {
        byte[] ipc = r.readBlock(blockNumber);
        assertThat(ipc).as("block %d ipc", blockNumber).isNotNull();
        // Round-trip the IPC and check the block-number column.
        try (
            ArrowStreamReader sr = new ArrowStreamReader(
                new ByteArrayInputStream(ipc), allocator
            )
        ) {
            assertThat(sr.loadNextBatch()).isTrue();
            VectorSchemaRoot root = sr.getVectorSchemaRoot();
            BigIntVector v = (BigIntVector) root.getVector(Converter.BLOCK_NUMBER);
            assertThat(v.get(0))
                .as("returned block-number column for requested %d", blockNumber)
                .isEqualTo(blockNumber);
        }
    }

    private Path writeChunk(long chunkStart, long[] blockNumbers) throws Exception {
        Path file = Files.createTempFile("s3chunkreader-test-", ".arrow");
        Schema schema = converter.getBlockSchema();
        try (
            FileChannel ch = FileChannel.open(
                file,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
            StreamingChunkWriter w = new StreamingChunkWriter(allocator, schema, ch)
        ) {
            for (long n : blockNumbers) {
                w.appendBatch(converter.toBlockIpcBytes(allocator, makeBlock(n)));
            }
        }
        return file;
    }

    private EthBlock.Block makeBlock(long n) {
        EthBlock.Block b = new EthBlock.Block();
        b.setNumber("0x" + Long.toHexString(n));
        b.setHash("0xhash" + n);
        b.setParentHash("0xparent" + n);
        b.setTimestamp("0x" + Long.toHexString(n * 1000));
        b.setMiner("0xminer");
        b.setGasLimit("0x" + Long.toHexString(30_000_000));
        b.setGasUsed("0x0");
        b.setSize("0x100");
        b.setExtraData("0x");
        b.setTransactions(Collections.emptyList());
        return b;
    }
}
