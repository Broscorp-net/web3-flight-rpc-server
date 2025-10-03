package net.broscorp.web3.server;

import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.cache.S3BlockLogsCache;
import net.broscorp.web3.cache.scheduler.S3CacheUploadScheduler;
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
import org.web3j.protocol.websocket.WebSocketService;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private static String flightPortString = System.getenv("FLIGHT_PORT");
    private static String ethereumNodeUrl = System.getenv("ETHEREUM_NODE_URL");
    private static String maxBlockRangeString = System.getenv("MAX_BLOCK_RANGE");
    private static String cacheBucketName = System.getenv("CACHE_BUCKET");
    private static String cacheFileKey = System.getenv("CACHE_FILE_KEY");
    private static String defaultRegion = System.getenv("AWS_DEFAULT_REGION");

    public static void main(String[] args) {
        runBlocksAndLogsProducer(args);
    }

    private static void runBlocksAndLogsProducer(String[] args) {

        if (args.length == 5) {
            ethereumNodeUrl = args[0];
            flightPortString = args[1];
            maxBlockRangeString = args[2];
            cacheBucketName = args[3];
            defaultRegion = args[4];
        }

        int flightPort = flightPortString == null ? 8815 : Integer.parseInt(flightPortString);

        if (ethereumNodeUrl == null || ethereumNodeUrl.isBlank()) {
            log.error("ETHEREUM_NODE_URL is null");
            throw new IllegalStateException("ETHEREUM_NODE_URL environment variable must be set");
        }

        if (cacheBucketName == null || cacheBucketName.isBlank()) {
            log.error("CACHE_BUCKET is null");
            throw new IllegalStateException("CACHE_BUCKET environment variable must be set");
        }

        if (defaultRegion == null || defaultRegion.isBlank()) {
            log.error("AWS_DEFAULT_REGION is null");
            throw new IllegalStateException("AWS_DEFAULT_REGION environment variable must be set");
        }

        log.info("Starting Ethereum to Arrow Flight Server, node url: {}", ethereumNodeUrl);

        Location serverLocation = Location.forGrpcInsecure("0.0.0.0", flightPort);

        while (true) {
            try {
                startFlightServer(serverLocation);
                break; // normal exit (server.awaitTermination ended)
            } catch (IOException e) {
                if (e.getMessage().contains("Connection was closed") || e.getMessage().contains("Realtime subscription error")) {
                    log.error("Websocket connection failed, retrying in 5s", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.error("Unexpected IO Exception", e);
                    break;
                }
            } catch (Exception e) {
                log.error("Server failed: ", e);
                break;
            }
        }
    }

    private static void startFlightServer(Location serverLocation) throws Exception {
        int maxBlockRange = maxBlockRangeString == null ? 500 : Integer.parseInt(maxBlockRangeString);
        S3BlockLogsCache s3BlockLogsCache = new S3BlockLogsCache(cacheBucketName, cacheFileKey, defaultRegion);
        S3CacheUploadScheduler uploader = new S3CacheUploadScheduler(s3BlockLogsCache);
        uploader.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            uploader.stop();
            s3BlockLogsCache.shutdown();
        }));


        WebSocketService webSocketService = null;
        Web3j web3 = null;

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
             BufferAllocator allocator = new RootAllocator()) {

            webSocketService = new WebSocketService(ethereumNodeUrl, true);
            webSocketService.connect();

            web3 = Web3j.build(webSocketService);

            try (FlightServer server = FlightServer.builder()
                    .allocator(allocator)
                    .location(serverLocation)
                    .producer(new Producer(
                            new LogsService(web3, s3BlockLogsCache, maxBlockRange, webSocketService),
                            // TODO Implement caching and batch size for BlockService
                            new BlocksService(web3, maxBlockRange),
                            new SubscriptionFactory(allocator, new Converter(), executorService)))
                    .build()) {

                server.start();
                log.info("Flight server started on {}", server.getLocation().getUri());

                server.awaitTermination();
            }
        } finally {
            if (webSocketService != null) {
                try {
                    webSocketService.close();
                } catch (Exception e) {
                    log.warn("Error closing WebSocketService", e);
                }
            }
            if (web3 != null) {
                web3.shutdown();
            }
            uploader.stop();
            s3BlockLogsCache.uploadSnapshot();
        }
    }
}
