package net.broscorp.web3.subscription;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
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
        metrics.subscriptionsTotal.labels(datasetName()).inc();
        metrics.subscriptionsActive.labels(datasetName()).inc();
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
        } catch (Exception e) {
            metrics.subscriptionErrorsTotal.labels(datasetName()).inc();
            log.error("Error in sequential subscription loop", e);
            listener.error(e);
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
        long chunkStart = ArchiveKey.chunkStartFor(blockNumber);
        if (currentChunkReader == null || currentChunkReader.chunkStart() != chunkStart) {
            closeChunkReader();
            currentChunkReader =
                archive.openChunkReader(datasetName(), chunkStart).get();
            if (currentChunkReader == null) {
                throw new IllegalStateException(
                    "Block " + blockNumber + " is pruned but archive chunk ["
                        + chunkStart + ", " + (chunkStart + ArchiveKey.CHUNK_SIZE)
                        + ") does not exist (dataset=" + datasetName() + ")"
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

    @Override
    public void close() throws Exception {
        if (isTerminated.compareAndSet(false, true)) {
            closeChunkReader();
            metrics.subscriptionsActive.labels(datasetName()).dec();
            AutoCloseables.close(root, allocator);
        }
    }
}
