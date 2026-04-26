package net.broscorp.web3.backfill.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.archive.ArchiveKey;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Writes assembled chunk bytes to S3 using the same key layout as the live
 * archive sweep ({@link ArchiveKey#objectKey}). Idempotency-friendly: callers
 * are expected to {@link #chunkExists} before assembling, so re-runs are cheap.
 */
@Slf4j
public class S3ChunkWriter implements AutoCloseable {

    private final S3Client s3;
    private final String bucket;
    private final String keyPrefix;

    public S3ChunkWriter(S3Client s3, String bucketWithOptionalPrefix) {
        this.s3 = s3;
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
        try {
            s3.headObject(
                HeadObjectRequest.builder().bucket(bucket).key(key).build()
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public void writeChunkFromFile(
        String dataset,
        long startBlock,
        long endBlock,
        Path file
    ) throws IOException {
        String key = ArchiveKey.objectKey(keyPrefix, dataset, startBlock, endBlock);
        long size = Files.size(file);
        s3.putObject(
            PutObjectRequest.builder().bucket(bucket).key(key).build(),
            RequestBody.fromFile(file)
        );
        log.info("Uploaded {} ({} bytes)", key, size);
    }

    @Override
    public void close() {
        s3.close();
    }
}
