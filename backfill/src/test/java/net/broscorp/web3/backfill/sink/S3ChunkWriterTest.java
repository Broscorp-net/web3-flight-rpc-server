package net.broscorp.web3.backfill.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class S3ChunkWriterTest {

    private S3Client s3;
    private Path tmpFile;

    @BeforeEach
    void setUp() throws IOException {
        s3 = mock(S3Client.class);
        tmpFile = Files.createTempFile("s3-chunk-writer-test-", ".arrow");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tmpFile);
    }

    @Test
    void chunkExists_returnsFalseWhenObjectMissing() {
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenThrow(NoSuchKeyException.builder().message("not found").build());
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket/prefix", 1000L);

        assertThat(writer.chunkExists("blocks", 0L, 1000L)).isFalse();
    }

    @Test
    void chunkExists_returnsFalseWhenObjectIsBelowFloor() {
        // Floor: 1000 bytes/block × 1000 blocks = 1_000_000 minimum.
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(
                HeadObjectResponse.builder().contentLength(500_000L).build()
            );
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket/prefix", 1000L);

        assertThat(writer.chunkExists("blocks", 0L, 1000L)).isFalse();
    }

    @Test
    void chunkExists_returnsTrueWhenObjectIsAtOrAboveFloor() {
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(
                HeadObjectResponse.builder().contentLength(1_000_000L).build()
            );
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket/prefix", 1000L);

        assertThat(writer.chunkExists("blocks", 0L, 1000L)).isTrue();
    }

    @Test
    void chunkExists_floorOfZeroDisablesTheCheck() {
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(
                HeadObjectResponse.builder().contentLength(1L).build()
            );
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket/prefix", 0L);

        assertThat(writer.chunkExists("blocks", 0L, 1000L)).isTrue();
    }

    @Test
    void writeChunkFromFile_succeedsWhenHeadSizeMatches() throws IOException {
        Files.write(tmpFile, new byte[1024]);
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(
                HeadObjectResponse.builder().contentLength(1024L).build()
            );
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket", 0L);

        writer.writeChunkFromFile("blocks", 0L, 1000L, tmpFile);

        verify(s3, times(1))
            .putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(s3, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void writeChunkFromFile_throwsWhenHeadSizeMismatches() throws IOException {
        Files.write(tmpFile, new byte[1024]);
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());
        when(s3.headObject(any(HeadObjectRequest.class)))
            .thenReturn(
                HeadObjectResponse.builder().contentLength(512L).build()
            );
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket", 0L);

        assertThatThrownBy(() ->
            writer.writeChunkFromFile("blocks", 0L, 1000L, tmpFile)
        )
            .isInstanceOf(IOException.class)
            .hasMessageContaining("integrity check failed")
            .hasMessageContaining("expected 1024")
            .hasMessageContaining("S3 reports 512");
    }

    @Test
    void writeChunkFromFile_doesNotHeadIfPutFails() throws IOException {
        Files.write(tmpFile, new byte[1024]);
        when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenThrow(new RuntimeException("upload failed"));
        S3ChunkWriter writer = new S3ChunkWriter(s3, "bucket", 0L);

        assertThatThrownBy(() ->
            writer.writeChunkFromFile("blocks", 0L, 1000L, tmpFile)
        ).isInstanceOf(RuntimeException.class);
        verify(s3, never()).headObject(any(HeadObjectRequest.class));
    }
}
