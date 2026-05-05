package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;

import io.reactivex.Flowable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlock;

class BlockchainIngestorTest {

    private Path tmpDir;
    private BlockchainCache cache;
    private BufferAllocator allocator;
    private Web3j web3jMock;
    private Converter converter;
    private Metrics metrics;

    @BeforeEach
    void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("ingestor-test");
        metrics = Metrics.forTesting();
        cache = new BlockchainCache(tmpDir.toString(), metrics);
        allocator = new RootAllocator();
        converter = new Converter();
        web3jMock = Mockito.mock(Web3j.class);
        Mockito.when(web3jMock.newHeadsNotifications()).thenReturn(Flowable.empty());
    }

    @AfterEach
    void tearDown() throws Exception {
        cache.close();
        allocator.close();
        deleteRecursively(tmpDir);
    }

    @Test
    void commitsBlocksInOrder_despiteOutOfOrderFetchCompletions()
        throws Exception {
        ConcurrentHashMap<Long, CompletableFuture<FullBlockData>> futures =
            new ConcurrentHashMap<>();
        for (long n = 0; n < 5; n++) {
            futures.put(n, new CompletableFuture<>());
        }

        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.valueOf(4));
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                return futures.get(blockNumber.longValue());
            }
        };

        BlockchainIngestor ingestor = new BlockchainIngestor(
            provider, cache, converter, allocator, web3jMock, null, metrics, 8
        );
        ingestor.start(0L, null, null);

        ScheduledExecutorService sched = Executors.newScheduledThreadPool(2);
        try {
            // Complete out of order: 2, 4, 0, 3, 1
            sched.schedule(() -> futures.get(2L).complete(makeBlock(2)), 50, TimeUnit.MILLISECONDS);
            sched.schedule(() -> futures.get(4L).complete(makeBlock(4)), 80, TimeUnit.MILLISECONDS);
            sched.schedule(() -> futures.get(0L).complete(makeBlock(0)), 120, TimeUnit.MILLISECONDS);
            sched.schedule(() -> futures.get(3L).complete(makeBlock(3)), 150, TimeUnit.MILLISECONDS);
            sched.schedule(() -> futures.get(1L).complete(makeBlock(1)), 200, TimeUnit.MILLISECONDS);

            awaitUntil(() -> cache.getLastIngestedBlock() == 4, 5000);
        } finally {
            sched.shutdownNow();
            ingestor.close();
        }

        for (long n = 0; n <= 4; n++) {
            assertThat(cache.getBlock(n)).isPresent();
            assertThat(cache.getLogs(n)).isPresent();
        }
    }

    @Test
    void forceInitialBlock_fastForwardsWarmCache() throws Exception {
        // Warm cache at lastIngestedBlock=2; pretend chain head is below the
        // forced start so the dispatcher can't progress and we observe
        // post-fast-forward meta cleanly.
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });
        assertThat(cache.getLastIngestedBlock()).isEqualTo(2);

        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.valueOf(9));
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                return new CompletableFuture<>();
            }
        };

        BlockchainIngestor ingestor = new BlockchainIngestor(
            provider, cache, converter, allocator, web3jMock, null, metrics, 1
        );
        try {
            ingestor.start(
                10L,
                null,
                null,
                BlockchainIngestor.DEFAULT_BACKFILL_BLOCKS,
                net.broscorp.web3.archive.ArchiveKey.DEFAULT_CHUNK_SIZE,
                true
            );

            assertThat(cache.getLastIngestedBlock()).isEqualTo(9);
            assertThat(cache.getPruneFloor()).isEqualTo(10);
            assertThat(cache.getForwardStart()).isEqualTo(10);
        } finally {
            ingestor.close();
        }
    }

    @Test
    void forceInitialBlock_isNoopWhenCacheAlreadyPastInitialBlock()
        throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });
        cache.commit(3, new byte[] { 3 }, new byte[] { 7 });

        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.valueOf(3));
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                return new CompletableFuture<>();
            }
        };

        BlockchainIngestor ingestor = new BlockchainIngestor(
            provider, cache, converter, allocator, web3jMock, null, metrics, 1
        );
        try {
            // initialBlock=2 but resumeFrom=4 (>= 2): flag is a no-op.
            ingestor.start(
                2L,
                null,
                null,
                BlockchainIngestor.DEFAULT_BACKFILL_BLOCKS,
                net.broscorp.web3.archive.ArchiveKey.DEFAULT_CHUNK_SIZE,
                true
            );

            assertThat(cache.getLastIngestedBlock()).isEqualTo(3);
            assertThat(cache.getPruneFloor()).isEqualTo(0);
        } finally {
            ingestor.close();
        }
    }

    private void awaitUntil(java.util.function.BooleanSupplier cond, long timeoutMs)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
        throw new AssertionError("timed out waiting for condition");
    }

    private FullBlockData makeBlock(long n) {
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
        return new FullBlockData(b, Collections.emptyList(), Collections.emptyMap());
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) return;
        try (var stream = Files.walk(path)) {
            stream
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> p.toFile().delete());
        }
    }
}
