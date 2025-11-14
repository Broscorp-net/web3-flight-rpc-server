package net.broscorp.web3.server;

import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.producer.Producer;
import net.broscorp.web3.service.BlocksService;
import net.broscorp.web3.service.LogsService;
import net.broscorp.web3.subscription.SubscriptionFactory;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.websocket.WebSocketService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * An app allowing Ethereum JSON-rpc to Apache Arrow Flight integration
 * <p>
 * This app:
 * <p>
 *     <ol>
 *         <li>
 *             Connects to an Ethereum node and subscribes to new logs and blocks according to client requests.
 *         </li>
 *         <li>
 *             Converts Ethereum data to Apache Arrow format
 *         </li>
 *         <li>
 *             Serves the Arrow data through an Arrow Flight server
 *         </li>
 *     </ol>
 * <p>
 * This is the 'dirty' config-filled component for now.
 */
@Slf4j
public class FlightRpcServer {

    public static void main(String[] args) {
        runBlocksAndLogsProducer(args);
    }

    private static void runBlocksAndLogsProducer(String[] args) {
        String flightPortString = System.getenv("FLIGHT_PORT");
        String ethereumNodeUrl = System.getenv("ETHEREUM_NODE_URL");
        String ethereumNodeHttpUrl = System.getenv("ETHEREUM_NODE_HTTP_URL");
        String maxBlockRangeString = System.getenv("MAX_BLOCK_RANGE");

        if (args.length == 3) {
            ethereumNodeUrl = args[0];
            flightPortString = args[1];
            maxBlockRangeString = args[2];
        }

        int flightPort = flightPortString == null ? 8815 : Integer.parseInt(flightPortString);
        int maxBlockRange = maxBlockRangeString == null ? 500 : Integer.parseInt(maxBlockRangeString);

        if (ethereumNodeUrl == null) {
            log.error("ETHEREUM_NODE_URL is null");
            System.exit(-1);
        }

        log.info("Starting Ethereum to Arrow Flight Server, node url: {}", ethereumNodeUrl);

        Location serverLocation = Location.forGrpcInsecure("0.0.0.0", flightPort);

        WebSocketService webSocketService = new WebSocketService(ethereumNodeUrl, true);
        try {
            webSocketService.connect();
        } catch (Exception e) {
            log.error("Failed to connect to Ethereum WebSocket node: {}", e.getMessage(), e);
            System.exit(-1);
        }

        Web3j web3WebSocket = Web3j.build(webSocketService);
        Supplier<Web3j> web3jHttpFactory = () -> Web3j.build(new HttpService(ethereumNodeHttpUrl));

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
             BufferAllocator allocator = new RootAllocator();
             FlightServer server = FlightServer.builder()
                     .allocator(allocator)
                     .location(serverLocation)
                     .producer(new Producer(new LogsService(web3WebSocket, web3jHttpFactory, maxBlockRange, webSocketService),
                             new BlocksService(web3WebSocket, maxBlockRange),
                             new SubscriptionFactory(allocator, new Converter(), executorService)))
                     .build()) {


            server.start();

            Location location = server.getLocation();
            log.info("Flight server started on {}", location.getUri());

            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                log.error("Flight server interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to start Flight server", e);
        } finally {
            webSocketService.close();
        }
    }
}
