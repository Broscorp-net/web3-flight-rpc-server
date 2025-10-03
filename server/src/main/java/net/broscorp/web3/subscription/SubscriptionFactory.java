package net.broscorp.web3.subscription;

import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.dto.request.LogsRequest;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.ExecutorService;

/**
 * Component responsible for building an appropriate Subscription
 * to Ethereum data based on the request it receives.
 */
public class SubscriptionFactory {
    private final BufferAllocator rootAllocator;
    private final Converter converter;
    private final ExecutorService executor;

    public SubscriptionFactory(BufferAllocator rootAllocator,
                               Converter converter,
                               ExecutorService executor) {
        this.rootAllocator = rootAllocator;
        this.converter = converter;
        this.executor = executor;
    }

    /**
     * Creates a subscription to Log data on the Ethereum blockchain.
     */
    public LogSubscription create(FlightProducer.ServerStreamListener listener,
                                  LogsRequest clientRequest) {
        BufferAllocator subscriptionAllocator = rootAllocator.newChildAllocator("log-sub-" + System.nanoTime(), 0, Long.MAX_VALUE);
        return new LogSubscription(listener,
                VectorSchemaRoot.create(converter.getLogSchema(), subscriptionAllocator),
                clientRequest,
                subscriptionAllocator,
                converter,
                executor);
    }

    /**
     * Creates a subscription to Block data on the Ethereum blockchain.
     */
    public BlockSubscription create(FlightProducer.ServerStreamListener listener,
                                    BlocksRequest clientRequest) {
        BufferAllocator subscriptionAllocator = rootAllocator.newChildAllocator("block-sub-" + System.nanoTime(), 0, Long.MAX_VALUE);
        return new BlockSubscription(listener,
                VectorSchemaRoot.create(converter.getBlockSchema(), subscriptionAllocator),
                clientRequest,
                subscriptionAllocator,
                converter,
                executor);
    }
}
