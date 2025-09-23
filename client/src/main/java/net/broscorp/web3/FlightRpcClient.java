package net.broscorp.web3;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;

/**
 * An example of an Arrow Flight client that consumes a stream
 * of new Ethereum transactions and outputs it in a tsv format to the console.
 */
@Slf4j
public class FlightRpcClient {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 8815;
        if (args.length == 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        runLogsClientExample(Location.forGrpcInsecure(host, port));
    }

    /**
     * Starts an example of a client that consumes logs data from our Flight server.
     */
    private static void runLogsClientExample(Location serverLocation) {
        log.info("Starting Logs client.");

        try (BufferAllocator clientAllocator = new RootAllocator(Long.MAX_VALUE);
             FlightClient client = FlightClient.builder()
                     .allocator(clientAllocator)
                     .location(serverLocation)
                     .build()) {
            Ticket ticket = new Ticket("{\"dataset\": \"logs\", \"startBlock\": \"null\", \"endBlock\": \"null\"}".getBytes(StandardCharsets.UTF_8));

            log.info("Starting data stream...");
            try (FlightStream stream = client.getStream(ticket);
                 VectorSchemaRoot incomingRoot = stream.getRoot()) {
                while (stream.next()) { // Blocking until data is available.
                    log.info("Incoming root rows: {}", incomingRoot.getRowCount());

                    log.info("{}", incomingRoot.contentToTSVString());
                }
            }
        } catch (Exception e) {
            log.error("Error in client: {}", e.getMessage(), e);
        }
    }
}
