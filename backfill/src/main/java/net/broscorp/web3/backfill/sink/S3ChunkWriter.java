package net.broscorp.web3.backfill.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Writes assembled chunk bytes to S3 using the same key layout as the live
 * archive sweep ({@link ArchiveKey#objectKey}). Idempotency-friendly: callers
 * are expected to {@link #chunkExists} before assembling, so re-runs are cheap.
 *
 * <p>Two integrity gates protect against silent corruption:
 * <ul>
 *   <li>After every upload, {@link #writeChunkFromFile} HEADs the new object
 *       and refuses to return successfully unless {@code Content-Length}
 *       matches the local file size.</li>
 *   <li>{@link #chunkExists} treats any object below
 *       {@code minBytesPerBlock * (endBlock - startBlock)} as missing, so
 *       a previously-corrupted (e.g. all-sentinel-rows) chunk is re-backfilled
 *       instead of being skipped on retry.</li>
 * </ul>
 */
@Slf4j
public class S3ChunkWriter implements AutoCloseable {

    private final S3Client s3;
    private final String bucket;
    private final String keyPrefix;
    private final long minBytesPerBlock;

    public S3ChunkWriter(
        S3Client s3,
        String bucketWithOptionalPrefix,
        long minBytesPerBlock
    ) {
        if (minBytesPerBlock < 0) {
            throw new IllegalArgumentException(
                "minBytesPerBlock must be >= 0 (got " + minBytesPerBlock + ")"
            );
        }
        this.s3 = s3;
        this.minBytesPerBlock = minBytesPerBlock;
        int slash = bucketWithOptionalPrefix.indexOf('/');
        if (slash < 0) {
            this.bucket = bucketWithOptionalPrefix;
            this.keyPrefix = "";
        } else {
            this.bucket = bucketWithOptionalPrefix.substring(0, slash);
            String prefix = bucketWithOptionalPrefix.substring(slash + 1);
            this.keyPrefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
        }
    }

    public boolean chunkExists(String dataset, long startBlock, long endBlock) {
        String key = ArchiveKey.objectKey(keyPrefix, dataset, startBlock, endBlock);
        HeadObjectResponse head;
        try {
            head = s3.headObject(
                HeadObjectRequest.builder().bucket(bucket).key(key).build()
            );
        } catch (NoSuchKeyException e) {
            return false;
        }
        long size = head.contentLength() == null ? 0L : head.contentLength();
        long minSize = minBytesPerBlock * (endBlock - startBlock);
        if (size < minSize) {
            log.warn(
                "Chunk {} exists but is below floor ({} bytes < {} required for {} blocks) — treating as missing for re-backfill",
                key,
                size,
                minSize,
                endBlock - startBlock
            );
            return false;
        }
        return true;
    }

    public void writeChunkFromFile(
        String dataset,
        long startBlock,
        long endBlock,
        Path file
    ) throws IOException {
        String key = ArchiveKey.objectKey(keyPrefix, dataset, startBlock, endBlock);
        long expectedSize = Files.size(file);
        s3.putObject(
            PutObjectRequest.builder().bucket(bucket).key(key).build(),
            RequestBody.fromFile(file)
        );
        HeadObjectResponse head = s3.headObject(
            HeadObjectRequest.builder().bucket(bucket).key(key).build()
        );
        long actualSize = head.contentLength() == null ? -1L : head.contentLength();
        if (actualSize != expectedSize) {
            throw new IOException(
                "S3 upload integrity check failed for " + key
                    + ": expected " + expectedSize
                    + " bytes, S3 reports " + actualSize
            );
        }
        log.info("Uploaded {} ({} bytes, verified)", key, expectedSize);
    }

    @Override
    public void close() {
        s3.close();
    }
}
