package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import org.apache.arrow.memory.BufferAllocator;
import org.web3j.protocol.Web3j;

/**
 * Drives ingestion of full blocks into {@link BlockchainCache} with strict
 * monotonic commit order.
 *
 * <p>Architecture:
 * <ul>
 *   <li>A WSS subscriber maintains {@code headBlock} from {@code newHeads}.</li>
 *   <li>A <b>dispatcher</b> thread fetches blocks concurrently (up to
 *       {@code MAX_INFLIGHT}) and deposits completed results into a
 *       {@link TreeMap} pending buffer.</li>
 *   <li>A <b>committer</b> thread drains the pending buffer strictly in
 *       ascending block order, converts each block to Arrow IPC, and writes
 *       to the cache. This guarantees {@code lastIngestedBlock} equals the
 *       highest contiguous committed block.</li>
 *   <li>Archive + prune runs on a separate single-thread executor so S3 uploads
 *       never stall commit progress.</li>
 * </ul>
 */
@Slf4j
public class BlockchainIngestor implements AutoCloseable {

    public static final int DEFAULT_MAX_INFLIGHT = 16;
    public static final long ARCHIVE_CHUNK_SIZE = 1000;

    private final BlockchainProvider provider;
    private final BlockchainCache cache;
    private final Converter converter;
    private final BufferAllocator allocator;
    private final Web3j web3jWebSocket;
    private final Metrics metrics;
    private final int maxInflight;

    private final ExecutorService fetchPool;
    private final ExecutorService dispatcherExecutor =
        Executors.newSingleThreadExecutor(r ->
            newNamedDaemon(r, "ingestor-dispatcher")
        );
    private final ExecutorService committerExecutor =
        Executors.newSingleThreadExecutor(r ->
            newNamedDaemon(r, "ingestor-committer")
        );
    private final ExecutorService archiveExecutor =
        Executors.newSingleThreadExecutor(r ->
            newNamedDaemon(r, "ingestor-archive")
        );

    private final Lock pendingLock = new ReentrantLock();
    private final Condition pendingReady = pendingLock.newCondition();
    private final TreeMap<Long, FullBlockData> pending = new TreeMap<>();

    private final AtomicLong headBlock = new AtomicLong(-1);

    private Long retentionBlocks;
    private ArchiveManager archiveManager;
    private long archiveDispatchedUpTo;

    public BlockchainIngestor(
        BlockchainProvider provider,
        BlockchainCache cache,
        Converter converter,
        BufferAllocator allocator,
        Web3j web3jWebSocket,
        Metrics metrics
    ) {
        this(
            provider,
            cache,
            converter,
            allocator,
            web3jWebSocket,
            metrics,
            DEFAULT_MAX_INFLIGHT
        );
    }

    public BlockchainIngestor(
        BlockchainProvider provider,
        BlockchainCache cache,
        Converter converter,
        BufferAllocator allocator,
        Web3j web3jWebSocket,
        Metrics metrics,
        int maxInflight
    ) {
        this.provider = provider;
        this.cache = cache;
        this.converter = converter;
        this.allocator = allocator;
        this.web3jWebSocket = web3jWebSocket;
        this.metrics = metrics;
        this.maxInflight = maxInflight;
        this.fetchPool = Executors.newFixedThreadPool(maxInflight, r ->
            newNamedDaemon(r, "ingestor-fetch")
        );
    }

    public void start(
        Long initialBlock,
        Long retentionBlocks,
        ArchiveManager archiveManager
    ) {
        this.retentionBlocks = retentionBlocks;
        this.archiveManager = archiveManager;
        this.archiveDispatchedUpTo = cache.getPruneFloor();

        long resumeFrom = cache.getLastIngestedBlock() + 1;
        long startFrom;
        if (cache.getLastIngestedBlock() < 0 && initialBlock != null) {
            startFrom = initialBlock;
        } else {
            startFrom = resumeFrom;
            if (initialBlock != null && initialBlock != resumeFrom) {
                log.warn(
                    "initialBlock={} ignored; resuming from lastIngestedBlock+1={}",
                    initialBlock,
                    resumeFrom
                );
            }
        }

        log.info(
            "Starting ingestion from block {} (retention={}, archiving={}, maxInflight={})",
            startFrom,
            retentionBlocks,
            archiveManager != null,
            maxInflight
        );

        subscribeToHead();
        dispatcherExecutor.submit(() -> dispatchLoop(startFrom));
        committerExecutor.submit(() -> commitLoop(startFrom));
    }

    private void subscribeToHead() {
        // Use native WebSocket eth_subscribe("newHeads") rather than
        // blockFlowable(), which sets up an HTTP-style eth_newBlockFilter poll
        // and triggers a QUANTITY encoding incompatibility with some nodes
        // (notably Hardhat rejecting "0x01" as a filter id).
        web3jWebSocket
            .newHeadsNotifications()
            .subscribe(
                notif -> {
                    String hexNumber = notif.getParams().getResult().getNumber();
                    long newHead =
                        Long.parseLong(hexNumber.substring(2), 16);
                    long updated =
                        headBlock.accumulateAndGet(newHead, Math::max);
                    metrics.ingestorHeadBlock.set(updated);
                },
                err -> log.error("Error in newHeads subscription", err)
            );
    }

    private void dispatchLoop(long startFrom) {
        ExecutorCompletionService<Map.Entry<Long, FullBlockData>> cs =
            new ExecutorCompletionService<>(fetchPool);
        long nextToFetch = startFrom;
        int outstanding = 0;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                while (
                    outstanding < maxInflight &&
                    nextToFetch <= currentHead()
                ) {
                    final long n = nextToFetch++;
                    cs.submit(() -> Map.entry(n, fetchWithRetry(n)));
                    outstanding++;
                }

                if (outstanding == 0) {
                    // No head progress yet; sleep briefly.
                    Thread.sleep(500);
                    continue;
                }

                Future<Map.Entry<Long, FullBlockData>> f = cs.take();
                outstanding--;
                Map.Entry<Long, FullBlockData> entry = f.get();
                pendingLock.lock();
                try {
                    pending.put(entry.getKey(), entry.getValue());
                    metrics.ingestorPendingDepth.set(pending.size());
                    pendingReady.signalAll();
                } finally {
                    pendingLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Dispatcher loop terminated", e);
        }
    }

    private long currentHead() throws InterruptedException {
        long h = headBlock.get();
        if (h >= 0) return h;
        try {
            long latest = provider
                .getLatestBlockNumber()
                .get(10, TimeUnit.SECONDS)
                .longValue();
            long updated = headBlock.accumulateAndGet(latest, Math::max);
            metrics.ingestorHeadBlock.set(updated);
            return updated;
        } catch (Exception e) {
            log.warn("Failed to query latest block number", e);
            Thread.sleep(1000);
            return -1;
        }
    }

    private FullBlockData fetchWithRetry(long blockNumber)
        throws InterruptedException {
        long backoffMs = 100;
        Histogram.Timer timer = metrics.ingestorFetchDurationSeconds.startTimer();
        try {
            while (true) {
                try {
                    return provider
                        .fetchFullBlock(BigInteger.valueOf(blockNumber))
                        .get(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (Exception e) {
                    metrics.ingestorFetchRetriesTotal.inc();
                    log.warn(
                        "Fetch failed for block {}; retrying in {}ms: {}",
                        blockNumber,
                        backoffMs,
                        e.toString()
                    );
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 10_000);
                }
            }
        } finally {
            timer.observeDuration();
        }
    }

    private void commitLoop(long startFrom) {
        long nextToCommit = startFrom;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                FullBlockData data;
                pendingLock.lock();
                try {
                    while (!pending.containsKey(nextToCommit)) {
                        pendingReady.await();
                    }
                    data = pending.remove(nextToCommit);
                    metrics.ingestorPendingDepth.set(pending.size());
                } finally {
                    pendingLock.unlock();
                }

                try {
                    commitBlock(nextToCommit, data);
                } catch (Exception e) {
                    metrics.ingestorCommitErrorsTotal.inc();
                    log.error(
                        "Commit failed for block {}; will not advance",
                        nextToCommit,
                        e
                    );
                    pendingLock.lock();
                    try {
                        pending.put(nextToCommit, data);
                        metrics.ingestorPendingDepth.set(pending.size());
                    } finally {
                        pendingLock.unlock();
                    }
                    Thread.sleep(500);
                    continue;
                }

                maybeArchive(nextToCommit);
                nextToCommit++;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void commitBlock(long blockNumber, FullBlockData data)
        throws Exception {
        byte[] blockIpc = converter.toBlockIpcBytes(allocator, data.block());
        byte[] logsIpc = converter.toLogIpcBytes(
            allocator,
            blockNumber,
            data.block().getHash(),
            data.block().getTimestamp().longValue(),
            data.logs(),
            data.receipts()
        );
        cache.commit(blockNumber, blockIpc, logsIpc);
        log.debug("Committed block {}", blockNumber);
    }

    private void maybeArchive(long committedBlock) {
        if (retentionBlocks == null) return;
        while (
            committedBlock >=
            archiveDispatchedUpTo + ARCHIVE_CHUNK_SIZE + retentionBlocks
        ) {
            final long start = archiveDispatchedUpTo;
            final long end = start + ARCHIVE_CHUNK_SIZE;
            archiveExecutor.submit(() -> runArchiveAndPrune(start, end));
            archiveDispatchedUpTo = end;
        }
    }

    private void runArchiveAndPrune(long start, long end) {
        try {
            if (archiveManager != null) {
                archiveManager.archiveRange(start, end).join();
            }
            cache.prune(end);
            log.info("Archive+prune complete for [{}, {})", start, end);
        } catch (Exception e) {
            log.error(
                "Archive/prune failed for [{}, {}); data remains in cache",
                start,
                end,
                e
            );
        }
    }

    private static Thread newNamedDaemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    @Override
    public void close() {
        dispatcherExecutor.shutdownNow();
        committerExecutor.shutdownNow();
        archiveExecutor.shutdown();
        fetchPool.shutdownNow();
        try {
            archiveExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
