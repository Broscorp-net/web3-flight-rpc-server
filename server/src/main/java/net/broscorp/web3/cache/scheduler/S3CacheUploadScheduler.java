package net.broscorp.web3.cache.scheduler;

import net.broscorp.web3.cache.RestorableBlockLogsCache;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class S3CacheUploadScheduler {

    private final RestorableBlockLogsCache cache;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public S3CacheUploadScheduler(RestorableBlockLogsCache cache) {
        this.cache = cache;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("Uploading cache snapshot to S3...");
                cache.uploadSnapshot();
                System.out.println("Upload finished.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
