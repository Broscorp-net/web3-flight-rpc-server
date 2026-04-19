package net.broscorp.web3.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.dto.request.ClientRequest;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.subscription.SubscriptionFactory;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;

/**
 * Flight producer that serves Ethereum data sequentially.
 */
@Slf4j
public class Producer extends NoOpFlightProducer {

    private final SubscriptionFactory subscriptionFactory;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public Producer(SubscriptionFactory subscriptionFactory) {
        this.subscriptionFactory = subscriptionFactory;
    }

    @Override
    public void getStream(
        CallContext context,
        Ticket ticket,
        ServerStreamListener listener
    ) {
        try {
            ClientRequest request = MAPPER.readValue(
                ticket.getBytes(),
                ClientRequest.class
            );
            log.info("Parsed sequential request: {}", request);

            switch (request) {
                case LogsRequest logRequest -> subscriptionFactory
                    .create(listener, logRequest)
                    .start();
                case BlocksRequest blockRequest -> subscriptionFactory
                    .create(listener, blockRequest)
                    .start();
            }
        } catch (Exception e) {
            log.error("Failed to route incoming stream request", e);
            listener.error(e);
        }
    }

    @Override
    public FlightInfo getFlightInfo(
        CallContext context,
        FlightDescriptor descriptor
    ) {
        return new FlightInfo(
            null,
            descriptor,
            List.of(new FlightEndpoint(new Ticket(descriptor.getCommand()))),
            -1,
            -1
        );
    }
}
