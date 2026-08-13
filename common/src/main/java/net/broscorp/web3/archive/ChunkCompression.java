package net.broscorp.web3.archive;

import java.util.Locale;
import java.util.Optional;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.compression.NoCompressionCodec;

/**
 * Body-compression setting for archive chunks written to S3.
 *
 * <p>Compression is applied <b>inside</b> the Arrow IPC stream: each record
 * batch's data buffers are compressed individually and tagged in the batch
 * metadata. The object stays a valid Arrow IPC stream and the key layout is
 * unchanged ({@code <dataset>/<start>_<end>.arrow}), so:
 *
 * <ul>
 *   <li>the listing index and chunk-range parsing need no changes;</li>
 *   <li>a reader that passes {@link #readerFactory()} transparently handles
 *       both compressed and uncompressed chunks — the codec is per-batch
 *       metadata, so old and new objects coexist in one bucket;</li>
 *   <li>the writer stays streaming — no whole-object buffering.</li>
 * </ul>
 *
 * <p>The trade-off versus wrapping the whole object in gzip/zstd is ratio:
 * per-buffer compression can't exploit redundancy across buffers or batches.
 * In exchange, nothing else in the read path has to know compression exists.
 *
 * <p>Accepted {@link #parse} values: {@code none} (or {@code off}),
 * {@code lz4}, {@code zstd}, {@code zstd:<level>}.
 */
public record ChunkCompression(CodecType codec, Optional<Integer> level) {

    /** Zstd levels accepted by {@code zstd:<level>}. */
    private static final int MIN_ZSTD_LEVEL = 1;
    private static final int MAX_ZSTD_LEVEL = 22;

    /** No body compression — the historical (and default) behaviour. */
    public static final ChunkCompression NONE =
        new ChunkCompression(CodecType.NO_COMPRESSION, Optional.empty());

    public ChunkCompression {
        if (codec == null || level == null) {
            throw new IllegalArgumentException("codec and level must not be null");
        }
        if (level.isPresent() && codec != CodecType.ZSTD) {
            throw new IllegalArgumentException(
                "Compression level is only supported for zstd (got " + codec + ")"
            );
        }
    }

    /**
     * Parses a codec spec. {@code null}, blank, {@code none} and {@code off}
     * all yield {@link #NONE}.
     *
     * @throws IllegalArgumentException on an unknown codec or out-of-range level
     */
    public static ChunkCompression parse(String raw) {
        if (raw == null || raw.isBlank()) return NONE;
        String spec = raw.trim().toLowerCase(Locale.ROOT);
        int colon = spec.indexOf(':');
        String name = colon < 0 ? spec : spec.substring(0, colon);
        String levelPart = colon < 0 ? null : spec.substring(colon + 1);

        CodecType codec = switch (name) {
            case "none", "off", "no", "false" -> CodecType.NO_COMPRESSION;
            case "lz4", "lz4_frame" -> CodecType.LZ4_FRAME;
            case "zstd", "zst" -> CodecType.ZSTD;
            default -> throw new IllegalArgumentException(
                "Unknown compression codec: " + raw + " (expected none|lz4|zstd|zstd:<level>)"
            );
        };

        if (levelPart == null || levelPart.isBlank()) {
            return codec == CodecType.NO_COMPRESSION
                ? NONE
                : new ChunkCompression(codec, Optional.empty());
        }
        if (codec != CodecType.ZSTD) {
            throw new IllegalArgumentException(
                "Compression level is only supported for zstd (got " + raw + ")"
            );
        }
        int parsed;
        try {
            parsed = Integer.parseInt(levelPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid zstd compression level in " + raw + ": " + levelPart
            );
        }
        if (parsed < MIN_ZSTD_LEVEL || parsed > MAX_ZSTD_LEVEL) {
            throw new IllegalArgumentException(
                "zstd compression level must be between " + MIN_ZSTD_LEVEL
                    + " and " + MAX_ZSTD_LEVEL + " (got " + parsed + ")"
            );
        }
        return new ChunkCompression(CodecType.ZSTD, Optional.of(parsed));
    }

    public boolean enabled() {
        return codec != CodecType.NO_COMPRESSION;
    }

    /** Codec factory for the write path; a no-op factory when disabled. */
    public CompressionCodec.Factory writerFactory() {
        return enabled()
            ? CommonsCompressionFactory.INSTANCE
            : NoCompressionCodec.Factory.INSTANCE;
    }

    /**
     * Codec factory for the read path. Readers pass this unconditionally —
     * {@code VectorLoader} only consults it for batches that actually carry a
     * compression type, so uncompressed chunks are unaffected.
     */
    public static CompressionCodec.Factory readerFactory() {
        return CommonsCompressionFactory.INSTANCE;
    }

    @Override
    public String toString() {
        if (!enabled()) return "none";
        return level.map(l -> codec + ":" + l).orElseGet(codec::toString)
            .toLowerCase(Locale.ROOT);
    }
}
