package net.broscorp.web3.subscription;

import java.util.concurrent.ExecutorService;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Builds a Subscription per client request, wiring it to the shared cache
 * and (optional) cold archive.
 */
public class SubscriptionFactory {

    private final BufferAllocator rootAllocator;
    private final Converter converter;
    private final ExecutorService executor;
    private final BlockchainCache cache;
    private final ArchiveManager archive;
    private final Metrics metrics;

    public SubscriptionFactory(
        BufferAllocator rootAllocator,
        Converter converter,
        BlockchainCache cache,
        ArchiveManager archive,
        ExecutorService executor,
        Metrics metrics
    ) {
        this.rootAllocator = rootAllocator;
        this.converter = converter;
        this.cache = cache;
        this.archive = archive;
        this.executor = executor;
        this.metrics = metrics;
    }

    public SequentialLogSubscription create(
        FlightProducer.ServerStreamListener listener,
        LogsRequest clientRequest
    ) {
        BufferAllocator subAllocator = rootAllocator.newChildAllocator(
            "log-sub-" + System.nanoTime(),
            0,
            Long.MAX_VALUE
        );
        return new SequentialLogSubscription(
            listener,
            VectorSchemaRoot.create(converter.getLogSchema(), subAllocator),
            subAllocator,
            clientRequest,
            cache,
            archive,
            executor,
            metrics
        );
    }

    public SequentialBlockSubscription create(
        FlightProducer.ServerStreamListener listener,
        BlocksRequest clientRequest
    ) {
        BufferAllocator subAllocator = rootAllocator.newChildAllocator(
            "block-sub-" + System.nanoTime(),
            0,
            Long.MAX_VALUE
        );
        return new SequentialBlockSubscription(
            listener,
            VectorSchemaRoot.create(converter.getBlockSchema(), subAllocator),
            subAllocator,
            clientRequest,
            cache,
            archive,
            executor,
            metrics
        );
    }
}
