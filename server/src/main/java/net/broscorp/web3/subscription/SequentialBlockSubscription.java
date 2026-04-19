package net.broscorp.web3.subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import net.broscorp.web3.service.BlockchainCache.CacheResult;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

public class SequentialBlockSubscription
    extends SequentialSubscription<BlocksRequest>
{

    public SequentialBlockSubscription(
        FlightProducer.ServerStreamListener listener,
        VectorSchemaRoot root,
        BufferAllocator allocator,
        BlocksRequest clientRequest,
        BlockchainCache cache,
        ArchiveManager archive,
        ExecutorService executor,
        Metrics metrics
    ) {
        super(listener, root, allocator, clientRequest, cache, archive, executor, metrics);
    }

    @Override
    protected CacheResult getFromCache(long blockNumber) throws Exception {
        return cache.getBlockOrWait(blockNumber);
    }

    @Override
    protected String datasetName() {
        return ArchiveManager.DATASET_BLOCKS;
    }

    @Override
    protected void processBatch(VectorSchemaRoot batchRoot) {
        int rows = batchRoot.getRowCount();
        if (rows == 0) return;

        List<TransferPair> pairs = new ArrayList<>(
            batchRoot.getFieldVectors().size()
        );
        for (int i = 0; i < batchRoot.getFieldVectors().size(); i++) {
            pairs.add(
                batchRoot.getVector(i).makeTransferPair(root.getVector(i))
            );
        }

        root.allocateNew();
        for (int row = 0; row < rows; row++) {
            for (TransferPair tp : pairs) {
                tp.copyValueSafe(row, row);
            }
        }
        root.setRowCount(rows);
        listener.putNext();
        metrics.subscriptionBatchesSentTotal.labels(datasetName()).inc();
        root.clear();
    }
}
