package net.broscorp.web3.service;

import static org.assertj.core.api.Assertions.assertThat;

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
