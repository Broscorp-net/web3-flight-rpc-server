package net.broscorp.web3.service;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.prometheus.client.Histogram;
import io.reactivex.disposables.Disposable;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import org.apache.arrow.memory.BufferAllocator;
import org.rocksdb.RocksDBException;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.websocket.WebSocketService;

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

    public static final int DEFAULT_MAX_INFLIGHT = 5;
    public static final long DEFAULT_BACKFILL_BLOCKS = 0L;

    private static final long WATCHDOG_INTERVAL_SECONDS = 10;
    private static final long HEAD_STALE_THRESHOLD_NANOS =
        TimeUnit.SECONDS.toNanos(30);

    private final BlockchainProvider provider;
    private final BlockchainCache cache;
    private final Converter converter;
    private final BufferAllocator allocator;
    private final Web3j web3jWebSocket;
    private final WebSocketService wss;
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
    private final ExecutorService backfillExecutor =
        Executors.newSingleThreadExecutor(r ->
            newNamedDaemon(r, "ingestor-backfill")
        );
    private final ScheduledExecutorService watchdogExecutor =
        Executors.newSingleThreadScheduledExecutor(r ->
            newNamedDaemon(r, "ingestor-ws-watchdog")
        );
    private final ExecutorService reconnectExecutor =
        Executors.newSingleThreadExecutor(r ->
            newNamedDaemon(r, "ingestor-ws-reconnect")
        );

    private final Lock pendingLock = new ReentrantLock();
    private final Condition pendingReady = pendingLock.newCondition();
    private final TreeMap<Long, FullBlockData> pending = new TreeMap<>();

    private final AtomicLong headBlock = new AtomicLong(-1);
    private final AtomicLong lastHeadProgressNanos = new AtomicLong(0);
    private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
    private volatile Disposable headSubscription;

    private Long retentionBlocks;
    private ArchiveManager archiveManager;
    private long archiveDispatchedUpTo;

    public BlockchainIngestor(
        BlockchainProvider provider,
        BlockchainCache cache,
        Converter converter,
        BufferAllocator allocator,
        Web3j web3jWebSocket,
        WebSocketService wss,
        Metrics metrics
    ) {
        this(
            provider,
            cache,
            converter,
            allocator,
            web3jWebSocket,
            wss,
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
        WebSocketService wss,
        Metrics metrics,
        int maxInflight
    ) {
        this.provider = provider;
        this.cache = cache;
        this.converter = converter;
        this.allocator = allocator;
        this.web3jWebSocket = web3jWebSocket;
        this.wss = wss;
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
    ) throws RocksDBException {
        start(initialBlock, retentionBlocks, archiveManager, DEFAULT_BACKFILL_BLOCKS);
    }

    public void start(
        Long initialBlock,
        Long retentionBlocks,
        ArchiveManager archiveManager,
        long backfillBlocks
    ) throws RocksDBException {
        this.retentionBlocks = retentionBlocks;
        this.archiveManager = archiveManager;

        boolean freshCache = cache.getLastIngestedBlock() < 0;
        long resumeFrom = cache.getLastIngestedBlock() + 1;
        long startFrom;
        if (freshCache) {
            startFrom = (initialBlock != null)
                ? initialBlock
                : fetchLatestBlockBlocking();
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

        // Backfill window and floors are only configured on a fresh cache; on
        // warm restart we preserve the persisted pruneFloor / forwardStart so
        // already-backfilled blocks stay available and we don't accidentally
        // wipe the contiguous-committed range.
        long backfillFloor = startFrom;
        if (freshCache) {
            if (backfillBlocks > 0) {
                backfillFloor = Math.max(0L, startFrom - backfillBlocks);
            }
            // Round backfillFloor DOWN to the chunk boundary so the very first
            // archive sweep produces an aligned chunk. Without this, a fresh
            // start at a non-multiple-of-CHUNK_SIZE block lays down misaligned
            // chunks (e.g. 24964344_24965344) that future formula-based reads
            // can't find. Costs at most CHUNK_SIZE-1 extra blocks of backfill,
            // filled from S3 (if a prior aligned chunk exists) or RPC.
            long alignedFloor = ArchiveKey.chunkStartFor(backfillFloor);
            if (alignedFloor < backfillFloor) {
                log.info(
                    "Aligning fresh-cache backfillFloor: {} -> {} (extending {} "
                        + "blocks for chunk-aligned archive layout)",
                    backfillFloor,
                    alignedFloor,
                    backfillFloor - alignedFloor
                );
                backfillFloor = alignedFloor;
            }
            if (backfillFloor > cache.getPruneFloor()) {
                cache.prune(backfillFloor);
            }
            cache.setForwardStart(startFrom);
        }

        long pruneFloor = cache.getPruneFloor();
        long alignedPruneFloor = ArchiveKey.chunkStartFor(pruneFloor);
        if (alignedPruneFloor == pruneFloor) {
            this.archiveDispatchedUpTo = pruneFloor;
        } else {
            // Warm restart inherited a misaligned pruneFloor from an older
            // server run that wrote chunks at non-aligned boundaries. Round up
            // so future sweeps emit aligned chunks. Cache blocks below the new
            // floor will be pruned without producing a NEW aligned chunk; they
            // typically already exist in legacy misaligned chunks in S3 (only
            // recoverable once the cold-read path uses a listing index).
            long advanced = alignedPruneFloor + ArchiveKey.CHUNK_SIZE;
            log.warn(
                "pruneFloor {} is not aligned to chunk size {}; advancing "
                    + "archiveDispatchedUpTo to {}. Cache blocks [{}, {}) will "
                    + "be pruned without producing a new aligned chunk.",
                pruneFloor,
                ArchiveKey.CHUNK_SIZE,
                advanced,
                pruneFloor,
                advanced
            );
            this.archiveDispatchedUpTo = advanced;
        }

        log.info(
            "Starting ingestion from block {} (retention={}, archiving={}, maxInflight={}, backfill=[{}, {}))",
            startFrom,
            retentionBlocks,
            archiveManager != null,
            maxInflight,
            backfillFloor,
            startFrom
        );

        lastHeadProgressNanos.set(System.nanoTime());
        subscribeToHead();
        watchdogExecutor.scheduleAtFixedRate(
            this::watchdogTick,
            WATCHDOG_INTERVAL_SECONDS,
            WATCHDOG_INTERVAL_SECONDS,
            TimeUnit.SECONDS
        );
        dispatcherExecutor.submit(() -> dispatchLoop(startFrom));
        committerExecutor.submit(() -> commitLoop(startFrom));
        if (freshCache && backfillFloor < startFrom) {
            final long from = startFrom - 1;
            final long to = backfillFloor;
            backfillExecutor.submit(() -> backfillLoop(from, to));
        }
    }

    private long fetchLatestBlockBlocking() {
        while (true) {
            try {
                long latest = provider
                    .getLatestBlockNumber()
                    .get(10, TimeUnit.SECONDS)
                    .longValue();
                log.info("Fresh cache: starting from current head block {}", latest);
                return latest;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                    "Interrupted while resolving start block", e
                );
            } catch (Exception e) {
                log.warn(
                    "Failed to query latest block; retrying in 1s: {}",
                    e.toString()
                );
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                        "Interrupted while resolving start block", ie
                    );
                }
            }
        }
    }

    private void subscribeToHead() {
        // Use native WebSocket eth_subscribe("newHeads") rather than
        // blockFlowable(), which sets up an HTTP-style eth_newBlockFilter poll
        // and triggers a QUANTITY encoding incompatibility with some nodes
        // (notably Hardhat rejecting "0x01" as a filter id).
        Disposable old = headSubscription;
        if (old != null && !old.isDisposed()) {
            old.dispose();
        }
        headSubscription = web3jWebSocket
            .newHeadsNotifications()
            .subscribe(
                notif -> {
                    String hexNumber = notif.getParams().getResult().getNumber();
                    long newHead =
                        Long.parseLong(hexNumber.substring(2), 16);
                    long updated =
                        headBlock.accumulateAndGet(newHead, Math::max);
                    metrics.ingestorHeadBlock.set(updated);
                    lastHeadProgressNanos.set(System.nanoTime());
                },
                err -> {
                    log.warn(
                        "newHeads subscription errored ({}); scheduling WS reconnect",
                        err.toString()
                    );
                    scheduleReconnect();
                },
                () -> {
                    log.warn("newHeads subscription completed; scheduling WS reconnect");
                    scheduleReconnect();
                }
            );
    }

    private void scheduleReconnect() {
        if (wss == null) return;
        if (!reconnectInFlight.compareAndSet(false, true)) return;
        reconnectExecutor.submit(() -> {
            try {
                attemptReconnect();
            } finally {
                reconnectInFlight.set(false);
            }
        });
    }

    private void attemptReconnect() {
        try {
            wss.connect();
            subscribeToHead();
            lastHeadProgressNanos.set(System.nanoTime());
            log.info("WS reconnect succeeded");
        } catch (Exception e) {
            log.warn("WS reconnect failed: {}", e.toString());
        }
    }

    private void watchdogTick() {
        long last = lastHeadProgressNanos.get();
        if (last == 0) return;
        long stale = System.nanoTime() - last;
        if (stale < HEAD_STALE_THRESHOLD_NANOS) return;
        log.warn(
            "headBlock has not advanced for {}s; HTTP-polling and reconnecting WS",
            TimeUnit.NANOSECONDS.toSeconds(stale)
        );
        try {
            long latest = provider
                .getLatestBlockNumber()
                .get(10, TimeUnit.SECONDS)
                .longValue();
            long updated = headBlock.accumulateAndGet(latest, Math::max);
            metrics.ingestorHeadBlock.set(updated);
            lastHeadProgressNanos.set(System.nanoTime());
        } catch (Exception e) {
            log.warn("HTTP head poll failed: {}", e.toString());
        }
        scheduleReconnect();
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
            archiveDispatchedUpTo + ArchiveKey.CHUNK_SIZE + retentionBlocks
        ) {
            final long start = archiveDispatchedUpTo;
            final long end = start + ArchiveKey.CHUNK_SIZE;
            archiveExecutor.submit(() -> runArchiveAndPrune(start, end));
            archiveDispatchedUpTo = end;
        }
    }

    private void backfillLoop(long fromInclusive, long toInclusive) {
        log.info("Backfill starting: range [{}, {}]", toInclusive, fromInclusive);
        try {
            long n = fromInclusive;
            while (n >= toInclusive) {
                if (Thread.currentThread().isInterrupted()) return;
                long chunkStart = ArchiveKey.chunkStartFor(n);
                long firstInChunk = Math.max(toInclusive, chunkStart);
                backfillChunk(firstInChunk, n);
                n = chunkStart - 1;
            }
            log.info("Backfill complete: range [{}, {}]", toInclusive, fromInclusive);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Backfill interrupted");
        }
    }

    /**
     * Backfills {@code [firstBlock, lastBlock]} (both inclusive) into the
     * cache. Opens an archive reader the first time a block needs one and
     * keeps it as long as subsequent blocks fall within its chunk range;
     * a block past the reader's end (or a reader open failure) triggers a
     * fresh open. Any per-block read miss falls back to RPC.
     */
    private void backfillChunk(long firstBlock, long lastBlock)
        throws InterruptedException {
        ArchiveManager.ChunkReader blocksReader = null;
        ArchiveManager.ChunkReader logsReader = null;
        try {
            for (long n = firstBlock; n <= lastBlock; n++) {
                if (Thread.currentThread().isInterrupted()) return;
                try {
                    if (archiveManager != null
                        && (blocksReader == null || n >= blocksReader.chunkEnd())) {
                        closeQuietly(blocksReader);
                        closeQuietly(logsReader);
                        blocksReader = openReaderQuietly(ArchiveManager.DATASET_BLOCKS, n);
                        logsReader = openReaderQuietly(ArchiveManager.DATASET_LOGS, n);
                    }
                    if (blocksReader != null && logsReader != null
                        && n < blocksReader.chunkEnd() && n < logsReader.chunkEnd()) {
                        byte[] blockIpc = blocksReader.readBlock(n);
                        byte[] logsIpc = logsReader.readBlock(n);
                        if (blockIpc != null && logsIpc != null) {
                            cache.commitBackfill(n, blockIpc, logsIpc);
                            continue;
                        }
                        log.warn(
                            "Archive chunk missing block {}; falling back to RPC", n
                        );
                    }
                    backfillFromRpc(n);
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    log.error("Backfill failed for block {}; skipping", n, e);
                }
            }
        } finally {
            closeQuietly(blocksReader);
            closeQuietly(logsReader);
        }
    }

    private ArchiveManager.ChunkReader openReaderQuietly(String dataset, long blockNumber) {
        try {
            return archiveManager.openChunkReader(dataset, blockNumber).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            log.warn(
                "Archive open failed for {} block {}; falling back to RPC: {}",
                dataset, blockNumber, e.toString()
            );
            return null;
        }
    }

    private static void closeQuietly(ArchiveManager.ChunkReader r) {
        if (r != null) r.close();
    }

    private void backfillFromRpc(long n) throws Exception {
        FullBlockData data = fetchWithRetry(n);
        byte[] blockIpc = converter.toBlockIpcBytes(allocator, data.block());
        byte[] logsIpc = converter.toLogIpcBytes(
            allocator,
            n,
            data.block().getHash(),
            data.block().getTimestamp().longValue(),
            data.logs(),
            data.receipts()
        );
        cache.commitBackfill(n, blockIpc, logsIpc);
    }

    private void runArchiveAndPrune(long start, long end) {
        try {
            if (archiveManager != null) {
                archiveManager.archiveRange(start, end).join();
            }
            cache.prune(end);
            log.info("Archive+prune complete for [{}, {})", start, end);
        } catch (Throwable t) {
            // Unwrap CompletionException so OOMs (and other Errors) bubbling
            // out of the s3-archive executor are detected.
            Throwable root = t;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            if (root instanceof OutOfMemoryError) {
                log.error(
                    "OutOfMemoryError during archive of [{}, {}); halting JVM "
                        + "for orchestrator restart",
                    start,
                    end,
                    root
                );
                // halt() skips shutdown hooks — they may also OOM.
                Runtime.getRuntime().halt(137);
                return;
            }
            log.error(
                "Archive/prune failed for [{}, {}); data remains in cache",
                start,
                end,
                t
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
        watchdogExecutor.shutdownNow();
        reconnectExecutor.shutdownNow();
        Disposable sub = headSubscription;
        if (sub != null && !sub.isDisposed()) sub.dispose();
        dispatcherExecutor.shutdownNow();
        committerExecutor.shutdownNow();
        backfillExecutor.shutdownNow();
        archiveExecutor.shutdown();
        fetchPool.shutdownNow();
        try {
            archiveExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
