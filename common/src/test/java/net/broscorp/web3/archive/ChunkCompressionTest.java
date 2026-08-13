package net.broscorp.web3.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ChunkCompressionTest {

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "none", "NONE", "off", " Off "})
    void unsetAndDisabledSpecsMeanNoCompression(String spec) {
        ChunkCompression c = ChunkCompression.parse(spec);
        assertThat(c).isEqualTo(ChunkCompression.NONE);
        assertThat(c.enabled()).isFalse();
        assertThat(c.toString()).isEqualTo("none");
    }

    @Test
    void parsesLz4() {
        ChunkCompression c = ChunkCompression.parse("LZ4");
        assertThat(c.codec()).isEqualTo(CodecType.LZ4_FRAME);
        assertThat(c.level()).isEmpty();
        assertThat(c.enabled()).isTrue();
    }

    @Test
    void parsesZstdWithoutLevel() {
        ChunkCompression c = ChunkCompression.parse("zstd");
        assertThat(c.codec()).isEqualTo(CodecType.ZSTD);
        assertThat(c.level()).isEmpty();
        assertThat(c.toString()).isEqualTo("zstd");
    }

    @Test
    void parsesZstdWithLevel() {
        ChunkCompression c = ChunkCompression.parse("zstd:9");
        assertThat(c.codec()).isEqualTo(CodecType.ZSTD);
        assertThat(c.level()).contains(9);
        assertThat(c.toString()).isEqualTo("zstd:9");
    }

    @ParameterizedTest
    @ValueSource(strings = {"gzip", "snappy", "zstd:0", "zstd:23", "zstd:abc", "lz4:5"})
    void rejectsInvalidSpecs(String spec) {
        assertThatThrownBy(() -> ChunkCompression.parse(spec))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsLevelOnNonZstdCodec() {
        assertThatThrownBy(
            () -> new ChunkCompression(CodecType.LZ4_FRAME, Optional.of(3))
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readerFactoryIsAlwaysCompressionCapable() {
        // Cold reads must decode chunks written by any config, so the read
        // factory does not depend on the current setting.
        assertThat(ChunkCompression.readerFactory())
            .isSameAs(ChunkCompression.parse("zstd").writerFactory());
    }
}
