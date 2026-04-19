package net.broscorp.web3.server;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.metrics.Metrics;
import net.broscorp.web3.producer.Producer;
import net.broscorp.web3.service.ArchiveManager;
import net.broscorp.web3.service.BlockchainCache;
import net.broscorp.web3.service.BlockchainIngestor;
import net.broscorp.web3.service.S3ArchiveManager;
import net.broscorp.web3.service.Web3jBlockchainProvider;
import net.broscorp.web3.subscription.SubscriptionFactory;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.websocket.WebSocketService;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
public class FlightRpcServer {

    enum ArchiveMode {
        /** Cold tier disabled; pruning deletes data permanently. Dev only. */
        OFF,
        /** Use S3 if fully configured, otherwise warn and fall back to OFF. */
        OPTIONAL,
        /** S3 must be configured; startup fails otherwise. Prod setting. */
        REQUIRED;

        static ArchiveMode parse(String raw) {
            if (raw == null || raw.isBlank()) return OPTIONAL;
            return switch (raw.trim().toLowerCase()) {
                case "off", "disabled" -> OFF;
                case "optional" -> OPTIONAL;
                case "required" -> REQUIRED;
                default -> throw new IllegalArgumentException(
                    "Invalid ARCHIVE_MODE: " + raw + " (expected off|optional|required)"
                );
            };
        }
    }

    public static void main(String[] args) {
        String flightPortString = System.getenv("FLIGHT_PORT");
        String ethereumNodeUrl = System.getenv("WEBSOCKET_NODE_URL");
        String ethereumNodeHttpUrl = System.getenv("HTTP_NODE_URL");
        String initialBlockString = System.getenv("INITIAL_BLOCK");
        String retentionBlocksString = System.getenv("RETENTION_BLOCKS");
        String dbPath = System.getenv("DB_PATH");

        String archiveModeString = System.getenv("ARCHIVE_MODE");
        String s3Bucket = System.getenv("S3_BUCKET");
        String s3Region = System.getenv("S3_REGION");
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY");
        String awsSecretKey = System.getenv("AWS_SECRET_KEY");

        String metricsPortString = System.getenv("METRICS_PORT");
        int metricsPort =
            metricsPortString == null ? 9091 : Integer.parseInt(metricsPortString);

        if (dbPath == null) dbPath = "rocksdb_cache";
        int flightPort =
            flightPortString == null ? 8815 : Integer.parseInt(flightPortString);
        Long initialBlock =
            initialBlockString != null ? Long.parseLong(initialBlockString) : null;
        Long retentionBlocks =
            retentionBlocksString != null
                ? Long.parseLong(retentionBlocksString)
                : null;
        ArchiveMode archiveMode = ArchiveMode.parse(archiveModeString);

        if (
            retentionBlocks != null &&
            retentionBlocks % BlockchainIngestor.ARCHIVE_CHUNK_SIZE != 0
        ) {
            log.error(
                "RETENTION_BLOCKS ({}) must be a multiple of ARCHIVE_CHUNK_SIZE ({})",
                retentionBlocks,
                BlockchainIngestor.ARCHIVE_CHUNK_SIZE
            );
            System.exit(-1);
        }

        boolean s3Configured =
            s3Bucket != null && awsAccessKey != null && awsSecretKey != null;
        if (archiveMode == ArchiveMode.REQUIRED && !s3Configured) {
            log.error(
                "ARCHIVE_MODE=required but S3 is not fully configured " +
                "(need S3_BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY)"
            );
            System.exit(-1);
        }
        if (archiveMode == ArchiveMode.OFF && retentionBlocks != null) {
            log.warn(
                "ARCHIVE_MODE=off with RETENTION_BLOCKS={}: pruned data will be lost permanently",
                retentionBlocks
            );
        }
        if (
            archiveMode == ArchiveMode.OPTIONAL &&
            retentionBlocks != null &&
            !s3Configured
        ) {
            log.warn(
                "ARCHIVE_MODE=optional, retention enabled, S3 not configured: " +
                "pruned data will be lost permanently (set ARCHIVE_MODE=required for prod)"
            );
        }

        if (ethereumNodeUrl == null || ethereumNodeHttpUrl == null) {
            log.error("WEBSOCKET_NODE_URL and HTTP_NODE_URL must be provided");
            System.exit(-1);
        }

        log.info("Starting Sequential Ethereum to Arrow Flight Server");
        Location serverLocation = Location.forGrpcInsecure("0.0.0.0", flightPort);

        WebSocketService blocksWss = new WebSocketService(ethereumNodeUrl, true);
        try {
            blocksWss.connect();
        } catch (Exception e) {
            log.error("Failed to connect to Ethereum WebSocket: {}", e.getMessage());
            System.exit(-1);
        }
        Web3j web3WebSocket = Web3j.build(blocksWss);
        Web3j web3Http = Web3j.build(new HttpService(ethereumNodeHttpUrl));

        Metrics metrics = Metrics.forDefaultRegistry();
        DefaultExports.initialize();
        HTTPServer metricsServer;
        try {
            metricsServer = new HTTPServer.Builder().withPort(metricsPort).build();
            log.info("Prometheus metrics exporter listening on :{}/metrics", metricsPort);
        } catch (Exception e) {
            log.error("Failed to start Prometheus exporter on port {}", metricsPort, e);
            System.exit(-1);
            return;
        }

        Converter converter = new Converter();
        try (
            ExecutorService executorService =
                Executors.newVirtualThreadPerTaskExecutor();
            BufferAllocator rootAllocator = new RootAllocator();
            BlockchainCache cache = new BlockchainCache(dbPath, metrics)
        ) {
            boolean useS3 = archiveMode != ArchiveMode.OFF && s3Configured;
            runServer(
                serverLocation,
                rootAllocator,
                cache,
                converter,
                executorService,
                web3WebSocket,
                web3Http,
                metrics,
                initialBlock,
                retentionBlocks,
                useS3 ? s3Bucket : null,
                s3Region,
                awsAccessKey,
                awsSecretKey
            );
        } catch (Exception e) {
            log.error("Failed to start Flight server", e);
        } finally {
            metricsServer.close();
            web3Http.shutdown();
            blocksWss.close();
        }
    }

    private static void runServer(
        Location serverLocation,
        BufferAllocator rootAllocator,
        BlockchainCache cache,
        Converter converter,
        ExecutorService executorService,
        Web3j web3WebSocket,
        Web3j web3Http,
        Metrics metrics,
        Long initialBlock,
        Long retentionBlocks,
        String s3Bucket,
        String s3Region,
        String awsAccessKey,
        String awsSecretKey
    ) throws Exception {
        BufferAllocator ingestorAllocator = rootAllocator.newChildAllocator(
            "ingestor", 0, Long.MAX_VALUE
        );
        BufferAllocator archiveAllocator = rootAllocator.newChildAllocator(
            "archive", 0, Long.MAX_VALUE
        );
        S3ArchiveManager s3ArchiveManager = null;
        ArchiveManager archiveManager = null;
        try {
            if (s3Bucket != null) {
                S3Client s3 = S3Client.builder()
                    .region(Region.of(s3Region != null ? s3Region : "us-east-1"))
                    .credentialsProvider(
                        StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(awsAccessKey, awsSecretKey)
                        )
                    )
                    .build();
                s3ArchiveManager = new S3ArchiveManager(
                    s3, s3Bucket, cache, converter, archiveAllocator, metrics
                );
                archiveManager = s3ArchiveManager;
                log.info("S3 archiving enabled for bucket: {}", s3Bucket);
            }

            Web3jBlockchainProvider provider = new Web3jBlockchainProvider(
                web3Http
            );
            try (
                BlockchainIngestor ingestor = new BlockchainIngestor(
                    provider,
                    cache,
                    converter,
                    ingestorAllocator,
                    web3WebSocket,
                    metrics
                )
            ) {
                ingestor.start(initialBlock, retentionBlocks, archiveManager);

                SubscriptionFactory subscriptionFactory = new SubscriptionFactory(
                    rootAllocator,
                    converter,
                    cache,
                    archiveManager,
                    executorService,
                    metrics
                );
                Producer producer = new Producer(subscriptionFactory);
                try (
                    FlightServer server = FlightServer.builder()
                        .allocator(rootAllocator)
                        .location(serverLocation)
                        .producer(producer)
                        .build()
                ) {
                    server.start();
                    log.info(
                        "Flight server started on {}",
                        server.getLocation().getUri()
                    );
                    server.awaitTermination();
                }
            }
        } finally {
            if (s3ArchiveManager != null) s3ArchiveManager.close();
            archiveAllocator.close();
            ingestorAllocator.close();
        }
    }
}
