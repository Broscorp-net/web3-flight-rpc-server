package net.broscorp.web3.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import net.broscorp.web3.archive.ArchiveKey;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import net.broscorp.web3.service.BlockchainCache.CacheResult;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SequentialSubscriptionTest {

    private BufferAllocator allocator;
    private BlockchainCache cache;
    private Converter converter;
    private ExecutorService executor;
    private Metrics metrics;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        cache = mock(BlockchainCache.class);
        converter = new Converter();
        executor = Executors.newSingleThreadExecutor();
        metrics = Metrics.forTesting();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
        executor.shutdown();
    }

    private record ListenerFixture(
        ServerStreamListener listener,
        AtomicInteger putNextCalls,
        AtomicInteger lastRowCount
    ) {}

    private ListenerFixture listenerFor(VectorSchemaRoot root) {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        AtomicInteger putNextCalls = new AtomicInteger(0);
        AtomicInteger lastRowCount = new AtomicInteger(0);
        doAnswer(inv -> {
            putNextCalls.incrementAndGet();
            lastRowCount.set(root.getRowCount());
            return null;
        }).when(listener).putNext();
        when(listener.isCancelled()).thenReturn(false);
        when(listener.isReady()).thenReturn(true);
        // Swallow start(...), setOnReadyHandler(...), error(...) no-ops left to Mockito default.
        return new ListenerFixture(listener, putNextCalls, lastRowCount);
    }

    @Test
    void processLoop_sendsBlocksSequentially() throws Exception {
        LogsRequest request = new LogsRequest();
        request.setStartBlock(BigInteger.valueOf(1));
        request.setEndBlock(BigInteger.valueOf(2));

        byte[] block1Logs = converter.toLogIpcBytes(
            allocator, 1L, "hash1", 100L, Collections.emptyList(), Collections.emptyMap()
        );
        byte[] block2Logs = converter.toLogIpcBytes(
            allocator, 2L, "hash2", 200L, Collections.emptyList(), Collections.emptyMap()
        );

        when(cache.getLogsOrWait(1L)).thenReturn(CacheResult.loaded(block1Logs));
        when(cache.getLogsOrWait(2L)).thenReturn(CacheResult.loaded(block2Logs));

        VectorSchemaRoot root = VectorSchemaRoot.create(
            converter.getLogSchema(), allocator
        );
        ListenerFixture lf = listenerFor(root);
        SequentialLogSubscription sub = new SequentialLogSubscription(
            lf.listener(), root, allocator, request, cache, null, executor, metrics
        );

        sub.start();
        Thread.sleep(500);

        assertThat(lf.putNextCalls().get()).isEqualTo(2);
    }

    @Test
    void logFiltering_filtersByAddress() throws Exception {
        LogsRequest request = new LogsRequest();
        request.setStartBlock(BigInteger.valueOf(1));
        request.setEndBlock(BigInteger.valueOf(1));
        request.setContractAddresses(List.of("0xMATCH"));

        org.web3j.protocol.core.methods.response.Log log1 =
            new org.web3j.protocol.core.methods.response.Log();
        log1.setAddress("0xMATCH");
        log1.setTransactionHash("0x1");
        log1.setLogIndex("0x1");
        log1.setTopics(Collections.emptyList());

        org.web3j.protocol.core.methods.response.Log log2 =
            new org.web3j.protocol.core.methods.response.Log();
        log2.setAddress("0xSKIP");
        log2.setTransactionHash("0x2");
        log2.setLogIndex("0x2");
        log2.setTopics(Collections.emptyList());

        byte[] logsIpc = converter.toLogIpcBytes(
            allocator, 1L, "hash1", 100L, List.of(log1, log2), Collections.emptyMap()
        );
        when(cache.getLogsOrWait(1L)).thenReturn(CacheResult.loaded(logsIpc));

        VectorSchemaRoot root = VectorSchemaRoot.create(
            converter.getLogSchema(), allocator
        );
        ListenerFixture lf = listenerFor(root);
        SequentialLogSubscription sub = new SequentialLogSubscription(
            lf.listener(), root, allocator, request, cache, null, executor, metrics
        );

        sub.start();
        Thread.sleep(500);

        assertThat(lf.putNextCalls().get()).isEqualTo(1);
        assertThat(lf.lastRowCount().get()).isEqualTo(1);
    }

    @Test
    void prunedBlock_fallsBackToArchive() throws Exception {
        LogsRequest request = new LogsRequest();
        request.setStartBlock(BigInteger.valueOf(5));
        request.setEndBlock(BigInteger.valueOf(5));

        byte[] archivedLogs = converter.toLogIpcBytes(
            allocator, 5L, "hash5", 500L, Collections.emptyList(), Collections.emptyMap()
        );

        when(cache.getLogsOrWait(5L)).thenReturn(CacheResult.pruned());
        ArchiveManager archive = mock(ArchiveManager.class);
        ArchiveManager.ChunkReader reader = mock(ArchiveManager.ChunkReader.class);
        long chunkStart = ArchiveKey.chunkStartFor(5L, ArchiveKey.DEFAULT_CHUNK_SIZE);
        when(reader.chunkEnd()).thenReturn(chunkStart + ArchiveKey.DEFAULT_CHUNK_SIZE);
        when(reader.readBlock(5L)).thenReturn(archivedLogs);
        when(archive.openChunkReader(ArchiveManager.DATASET_LOGS, 5L))
            .thenReturn(CompletableFuture.completedFuture(reader));

        VectorSchemaRoot root = VectorSchemaRoot.create(
            converter.getLogSchema(), allocator
        );
        ListenerFixture lf = listenerFor(root);
        SequentialLogSubscription sub = new SequentialLogSubscription(
            lf.listener(), root, allocator, request, cache, archive, executor, metrics
        );

        sub.start();
        Thread.sleep(500);

        assertThat(lf.putNextCalls().get()).isEqualTo(1);
    }

    @Test
    void prunedConsecutive_opensChunkReaderOnce() throws Exception {
        long chunkStart = ArchiveKey.chunkStartFor(5L, ArchiveKey.DEFAULT_CHUNK_SIZE);
        long block1 = chunkStart + 5;
        long block2 = chunkStart + 6;

        LogsRequest request = new LogsRequest();
        request.setStartBlock(BigInteger.valueOf(block1));
        request.setEndBlock(BigInteger.valueOf(block2));

        when(cache.getLogsOrWait(block1)).thenReturn(CacheResult.pruned());
        when(cache.getLogsOrWait(block2)).thenReturn(CacheResult.pruned());

        byte[] payload = converter.toLogIpcBytes(
            allocator, 0L, "h", 0L, Collections.emptyList(), Collections.emptyMap()
        );

        ArchiveManager archive = mock(ArchiveManager.class);
        ArchiveManager.ChunkReader reader = mock(ArchiveManager.ChunkReader.class);
        when(reader.chunkEnd()).thenReturn(chunkStart + ArchiveKey.DEFAULT_CHUNK_SIZE);
        when(reader.readBlock(block1)).thenReturn(payload);
        when(reader.readBlock(block2)).thenReturn(payload);
        when(archive.openChunkReader(ArchiveManager.DATASET_LOGS, block1))
            .thenReturn(CompletableFuture.completedFuture(reader));

        VectorSchemaRoot root = VectorSchemaRoot.create(
            converter.getLogSchema(), allocator
        );
        ListenerFixture lf = listenerFor(root);
        SequentialLogSubscription sub = new SequentialLogSubscription(
            lf.listener(), root, allocator, request, cache, archive, executor, metrics
        );

        sub.start();
        Thread.sleep(500);

        assertThat(lf.putNextCalls().get()).isEqualTo(2);
        // Two pruned reads from the same chunk should result in exactly ONE
        // S3 download, not two.
        verify(archive, times(1)).openChunkReader(
            eq(ArchiveManager.DATASET_LOGS), eq(block1)
        );
        // Reader is closed when the subscription terminates.
        verify(reader).close();
    }
}
