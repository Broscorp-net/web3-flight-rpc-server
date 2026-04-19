package net.broscorp.web3.subscription;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import net.broscorp.web3.service.BlockchainCache.CacheResult;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.TransferPair;

public class SequentialLogSubscription
    extends SequentialSubscription<LogsRequest>
{

    public SequentialLogSubscription(
        FlightProducer.ServerStreamListener listener,
        VectorSchemaRoot root,
        BufferAllocator allocator,
        LogsRequest clientRequest,
        BlockchainCache cache,
        ArchiveManager archive,
        ExecutorService executor,
        Metrics metrics
    ) {
        super(listener, root, allocator, clientRequest, cache, archive, executor, metrics);
    }

    @Override
    protected CacheResult getFromCache(long blockNumber) throws Exception {
        return cache.getLogsOrWait(blockNumber);
    }

    @Override
    protected String datasetName() {
        return ArchiveManager.DATASET_LOGS;
    }

    @Override
    protected void processBatch(VectorSchemaRoot batchRoot) {
        int inputRows = batchRoot.getRowCount();
        if (inputRows == 0) return;

        VarCharVector addressVector = (VarCharVector) batchRoot.getVector(
            Converter.LOG_ADDRESS
        );
        ListVector topicsVector = (ListVector) batchRoot.getVector(
            Converter.LOG_TOPICS
        );

        List<Integer> matchingIndices = new ArrayList<>();
        for (int i = 0; i < inputRows; i++) {
            if (
                isSentinel(batchRoot, i) ||
                matches(addressVector, topicsVector, i)
            ) {
                matchingIndices.add(i);
            }
        }

        if (matchingIndices.isEmpty()) return;

        List<TransferPair> pairs = new ArrayList<>(
            batchRoot.getFieldVectors().size()
        );
        for (int i = 0; i < batchRoot.getFieldVectors().size(); i++) {
            pairs.add(
                batchRoot.getVector(i).makeTransferPair(root.getVector(i))
            );
        }

        root.allocateNew();
        for (int dst = 0; dst < matchingIndices.size(); dst++) {
            int src = matchingIndices.get(dst);
            for (TransferPair tp : pairs) {
                tp.copyValueSafe(src, dst);
            }
        }
        root.setRowCount(matchingIndices.size());
        listener.putNext();
        metrics.subscriptionBatchesSentTotal.labels(datasetName()).inc();
        root.clear();
    }

    private boolean isSentinel(VectorSchemaRoot root, int i) {
        return root.getVector(Converter.LOG_ADDRESS).isNull(i);
    }

    private boolean matches(
        VarCharVector addressVector,
        ListVector topicsVector,
        int i
    ) {
        boolean addressMatch = true;
        if (
            clientRequest.getContractAddresses() != null &&
            !clientRequest.getContractAddresses().isEmpty()
        ) {
            addressMatch = false;
            if (!addressVector.isNull(i)) {
                String addr = new String(
                    addressVector.get(i),
                    StandardCharsets.UTF_8
                );
                for (String target : clientRequest.getContractAddresses()) {
                    if (target.equalsIgnoreCase(addr)) {
                        addressMatch = true;
                        break;
                    }
                }
            }
        }
        if (!addressMatch) return false;

        if (
            clientRequest.getTopics() != null &&
            !clientRequest.getTopics().isEmpty()
        ) {
            if (topicsVector.isNull(i)) return false;
            Object topics = topicsVector.getObject(i);
            if (topics instanceof List && !((List<?>) topics).isEmpty()) {
                String firstTopic = ((List<?>) topics).get(0).toString();
                return clientRequest.getTopics().contains(firstTopic);
            }
            return false;
        }

        return true;
    }
}
