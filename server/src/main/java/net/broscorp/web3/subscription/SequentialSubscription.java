package net.broscorp.web3.subscription;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.ClientRequest;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import net.broscorp.web3.service.BlockchainCache.CacheResult;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

@Slf4j
public abstract class SequentialSubscription<
    R extends ClientRequest
> implements AutoCloseable {

    protected final FlightProducer.ServerStreamListener listener;
    protected final VectorSchemaRoot root;
    protected final BufferAllocator allocator;
    protected final R clientRequest;
    protected final BlockchainCache cache;
    protected final ArchiveManager archive;
    protected final ExecutorService executor;
    protected final Metrics metrics;
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);
    private final Lock readyLock = new ReentrantLock();
    private final Condition readySignal = readyLock.newCondition();
    private ArchiveManager.ChunkReader currentChunkReader;

    protected SequentialSubscription(
        FlightProducer.ServerStreamListener listener,
        VectorSchemaRoot root,
        BufferAllocator allocator,
        R clientRequest,
        BlockchainCache cache,
        ArchiveManager archive,
        ExecutorService executor,
        Metrics metrics
    ) {
        this.listener = listener;
        this.root = root;
        this.allocator = allocator;
        this.clientRequest = clientRequest;
        this.cache = cache;
        this.archive = archive;
        this.executor = executor;
        this.metrics = metrics;

        listener.start(root);
        try {
            listener.setOnReadyHandler(this::onReady);
        } catch (UnsupportedOperationException e) {
            // Older or non-gRPC transport. We still poll isReady() with a
            // bounded await, so backpressure works without the callback —
            // just wakes up via the timeout instead of the signal.
            log.warn(
                "Flight transport has no setOnReadyHandler; "
                    + "backpressure will rely on bounded polling"
            );
        }
        metrics.subscriptionsTotal.labels(datasetName()).inc();
        metrics.subscriptionsActive.labels(datasetName()).inc();
    }

    /**
     * Invoked by gRPC (on a transport thread) when the stream's send window
     * reopens. Wakes any producer thread parked in {@link #awaitReady}.
     */
    private void onReady() {
        readyLock.lock();
        try {
            readySignal.signalAll();
        } finally {
            readyLock.unlock();
        }
    }

    /**
     * Parks the calling thread until the stream is ready for another
     * {@code putNext}, the client cancels, or the subscription terminates.
     * The bounded {@code await} timeout means we still recover without the
     * onReady callback (e.g. on transports that don't fire it) and re-check
     * cancel/terminate state regularly. Always observes the elapsed wait
     * into {@code flight_subscription_backpressure_seconds} — even on the
     * fast path (0s) — so the histogram count tracks putNext volume and
     * the sum / quantiles reflect actual stalls.
     */
    private void awaitReady() {
        long t0 = System.nanoTime();
        if (listener.isReady() || listener.isCancelled() || isTerminated.get()) {
            metrics.subscriptionBackpressureSeconds
                .labels(datasetName()).observe(0);
            return;
        }
        readyLock.lock();
        try {
            while (!listener.isReady()
                && !listener.isCancelled()
                && !isTerminated.get()) {
                try {
                    readySignal.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            readyLock.unlock();
        }
        long waitNs = System.nanoTime() - t0;
        metrics.subscriptionBackpressureSeconds
            .labels(datasetName()).observe(waitNs / 1_000_000_000.0);
        long waitMs = waitNs / 1_000_000L;
        if (waitMs >= 100) {
            log.info(
                "Backpressure: parked {}ms for stream ready (dataset={})",
                waitMs, datasetName()
            );
        }
    }

    public void start() {
        executor.submit(this::processLoop);
    }

    private void processLoop() {
        try {
            long currentBlock = resolveStartBlock();

            Long endBlock =
                clientRequest.getEndBlock() != null
                    ? clientRequest.getEndBlock().longValue()
                    : null;

            while (!isTerminated.get() && !listener.isCancelled()) {
                if (endBlock != null && currentBlock > endBlock) break;

                byte[] data = fetchIpcBytes(currentBlock);
                if (data == null) break;

                loadAndSend(data);
                currentBlock++;
            }

            if (!isTerminated.get() && !listener.isCancelled()) {
                listener.completed();
            }
        } catch (BackfillPendingException e) {
            metrics.subscriptionErrorsTotal.labels(datasetName()).inc();
            log.info("Subscription rejected: {}", e.getMessage());
            listener.error(
                CallStatus.UNAVAILABLE
                    .withDescription(e.getMessage())
                    .toRuntimeException()
            );
        } catch (Throwable t) {
            // Catch Throwable (not just Exception) so Errors — most importantly
            // OutOfMemoryError thrown by Netty/Arrow on direct-memory exhaustion
            // — surface to the client as a real gRPC error instead of vanishing
            // into a silent stream.
            metrics.subscriptionErrorsTotal.labels(datasetName()).inc();
            log.error("Error in sequential subscription loop", t);
            try {
                listener.error(t);
            } catch (Throwable ignore) {
                // Listener may itself fail when out of memory; we've done our
                // bookkeeping and the halt below still runs.
            }
            if (t instanceof OutOfMemoryError) {
                // -XX:+ExitOnOutOfMemoryError covers heap OOMs raised by the VM
                // but NOT direct-buffer OOMs thrown from java.nio.Bits in Java
                // code, and a re-throw here is swallowed by the virtual thread's
                // default uncaught handler. Halt explicitly so the container
                // restarts clean instead of leaking a pinned Netty pool until
                // the cgroup OOMKills us.
                log.error("OutOfMemoryError — halting JVM for clean restart");
                Runtime.getRuntime().halt(137);
            }
            if (t instanceof Error) throw (Error) t;
        } finally {
            try {
                close();
            } catch (Exception e) {
                log.error("Error closing subscription", e);
            }
        }
    }

    /**
     * If the client supplied {@code startBlock}, resume there. Otherwise, pick
     * up at the next block after the cache head (tail mode). On a fresh cache
     * the returned value is 0, and the cache's wait logic will block until the
     * first block is ingested.
     */
    private long resolveStartBlock() {
        if (clientRequest.getStartBlock() != null) {
            return clientRequest.getStartBlock().longValue();
        }
        return Math.max(0L, cache.getLastIngestedBlock() + 1);
    }

    private byte[] fetchIpcBytes(long blockNumber) throws Exception {
        CacheResult r = getFromCache(blockNumber);
        return switch (r.status()) {
            case LOADED -> {
                closeChunkReader();
                yield r.data();
            }
            case PRUNED -> fetchFromArchive(blockNumber);
            case BACKFILLING -> {
                closeChunkReader();
                throw new BackfillPendingException(blockNumber);
            }
        };
    }

    private byte[] fetchFromArchive(long blockNumber) throws Exception {
        if (archive == null) {
            throw new IllegalStateException(
                "Block " + blockNumber + " is pruned and no archive is configured"
            );
        }
        if (currentChunkReader == null || blockNumber >= currentChunkReader.chunkEnd()) {
            closeChunkReader();
            currentChunkReader =
                archive.openChunkReader(datasetName(), blockNumber).get();
            if (currentChunkReader == null) {
                throw new IllegalStateException(
                    "Block " + blockNumber
                        + " is pruned but no archive chunk covers it (dataset="
                        + datasetName() + ")"
                );
            }
        }
        byte[] data = currentChunkReader.readBlock(blockNumber);
        if (data == null) {
            throw new IllegalStateException(
                "Block " + blockNumber +
                " not present in archive chunk (dataset=" + datasetName() + ")"
            );
        }
        return data;
    }

    private void closeChunkReader() {
        if (currentChunkReader != null) {
            currentChunkReader.close();
            currentChunkReader = null;
        }
    }

    protected abstract CacheResult getFromCache(long blockNumber)
        throws Exception;

    protected abstract String datasetName();

    private void loadAndSend(byte[] data) throws IOException {
        try (
            ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(data),
                allocator
            )
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
            processBatch(batchRoot);
        }
    }

    protected abstract void processBatch(VectorSchemaRoot batchRoot);

    /**
     * Sends the current contents of {@link #root} to the client, parking the
     * caller first if the stream's send window is closed. Respecting
     * {@link FlightProducer.ServerStreamListener#isReady} is what keeps the
     * server-side gRPC outbound queue (backed by Netty direct memory) from
     * growing unboundedly when the consumer is slower than the producer.
     */
    protected void putNextTimed() {
        awaitReady();
        long startNs = System.nanoTime();
        listener.putNext();
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
        if (elapsedMs >= 100) {
            log.warn(
                "Slow putNext: {}ms (dataset={})",
                elapsedMs, datasetName()
            );
        }
    }

    @Override
    public void close() throws Exception {
        if (isTerminated.compareAndSet(false, true)) {
            closeChunkReader();
            metrics.subscriptionsActive.labels(datasetName()).dec();
            AutoCloseables.close(root, allocator);
        }
    }
}
