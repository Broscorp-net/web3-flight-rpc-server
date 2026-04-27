package net.broscorp.web3.archive;

/**
 * S3 cold-tier object-key layout.
 *
 * <p>Single source of truth for the key format used by every writer (live
 * ingestor archive sweep, batch backfill jobs) and reader (cold-tier
 * lookup) so they cannot drift. Chunk size is supplied by the caller —
 * it is configured per process via {@code ARCHIVE_CHUNK_SIZE} and may
 * differ across runs that share an S3 prefix; the read-side index
 * tolerates the resulting variable-size layout.
 */
public final class ArchiveKey {

    public static final long DEFAULT_CHUNK_SIZE = 1000;

    private ArchiveKey() {}

    /**
     * Lower bound (inclusive) of the chunk of size {@code chunkSize} that
     * contains {@code blockNumber}.
     */
    public static long chunkStartFor(long blockNumber, long chunkSize) {
        return (blockNumber / chunkSize) * chunkSize;
    }

    /**
     * Builds the object key for a chunk: {@code <keyPrefix><dataset>/<startBlock>_<endBlock>.arrow}
     * with {@code endBlock} exclusive.
     *
     * @param keyPrefix already terminated with {@code /} or empty
     */
    public static String objectKey(
        String keyPrefix,
        String dataset,
        long startBlock,
        long endBlock
    ) {
        return keyPrefix + dataset + "/" + startBlock + "_" + endBlock + ".arrow";
    }
}
