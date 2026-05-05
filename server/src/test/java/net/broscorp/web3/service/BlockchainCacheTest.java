package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.BlockchainCache.CacheResult;
import net.broscorp.web3.service.BlockchainCache.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockchainCacheTest {

    private Path tmpDir;
    private BlockchainCache cache;

    @BeforeEach
    void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("bc-cache-test");
        cache = new BlockchainCache(tmpDir.toString(), Metrics.forTesting());
    }

    @AfterEach
    void tearDown() throws Exception {
        cache.close();
        deleteRecursively(tmpDir);
    }

    @Test
    void commitAndGet_roundtrip() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });

        assertThat(cache.getBlock(1).orElseThrow()).containsExactly(1);
        assertThat(cache.getLogs(2).orElseThrow()).containsExactly(8);
        assertThat(cache.getLastIngestedBlock()).isEqualTo(2);
    }

    @Test
    void getBlockOrWait_returnsLoaded() throws Exception {
        cache.commit(3, new byte[] { 7 }, new byte[] { 6 });
        CacheResult r = cache.getBlockOrWait(3);
        assertThat(r.status()).isEqualTo(Status.LOADED);
        assertThat(r.data()).containsExactly(7);
    }

    @Test
    void getBlockOrWait_waitsUntilCommit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<CacheResult> result = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                started.countDown();
                result.set(cache.getBlockOrWait(10));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        waiter.start();
        started.await();
        Thread.sleep(100); // let waiter enter await

        cache.commit(10, new byte[] { 42 }, new byte[] { 43 });
        waiter.join(5000);

        assertThat(result.get()).isNotNull();
        assertThat(result.get().status()).isEqualTo(Status.LOADED);
        assertThat(result.get().data()).containsExactly(42);
    }

    @Test
    void prunedBlock_returnsPruned() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });
        cache.prune(2); // deletes blocks < 2, floor = 2

        CacheResult r = cache.getBlockOrWait(1);
        assertThat(r.status()).isEqualTo(Status.PRUNED);
        assertThat(cache.getPruneFloor()).isEqualTo(2);
    }

    @Test
    void fastForwardTo_advancesMetaWithoutDeletingData() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });
        cache.commit(3, new byte[] { 3 }, new byte[] { 7 });

        cache.fastForwardTo(100);

        assertThat(cache.getLastIngestedBlock()).isEqualTo(99);
        assertThat(cache.getPruneFloor()).isEqualTo(100);
        assertThat(cache.getForwardStart()).isEqualTo(100);

        // Underlying bytes remain on disk; raw getBlock/getLogs bypass meta checks.
        assertThat(cache.getBlock(2).orElseThrow()).containsExactly(2);
        assertThat(cache.getLogs(2).orElseThrow()).containsExactly(8);

        // But getOrWait reports them as PRUNED (below the new pruneFloor).
        CacheResult r = cache.getBlockOrWait(2);
        assertThat(r.status()).isEqualTo(Status.PRUNED);
    }

    @Test
    void fastForwardTo_persistsAcrossReopen() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.fastForwardTo(50);
        cache.close();

        cache = new BlockchainCache(tmpDir.toString(), Metrics.forTesting());
        assertThat(cache.getLastIngestedBlock()).isEqualTo(49);
        assertThat(cache.getPruneFloor()).isEqualTo(50);
        assertThat(cache.getForwardStart()).isEqualTo(50);
    }

    @Test
    void fastForwardTo_rejectsNonAdvancingBlock() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });

        // lastIngestedBlock=2, so fastForwardTo(3) would set it back to 2
        // (3 - 1 = 2). Reject — caller must guarantee strict advancement.
        assertThatThrownBy(() -> cache.fastForwardTo(3))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.fastForwardTo(2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void metaPersistsAcrossReopen() throws Exception {
        cache.commit(1, new byte[] { 1 }, new byte[] { 9 });
        cache.commit(2, new byte[] { 2 }, new byte[] { 8 });
        cache.prune(2);
        cache.close();

        cache = new BlockchainCache(tmpDir.toString(), Metrics.forTesting());
        assertThat(cache.getLastIngestedBlock()).isEqualTo(2);
        assertThat(cache.getPruneFloor()).isEqualTo(2);
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
