package net.broscorp.web3.archive;

/**
 * S3 cold-tier object-key layout.
 *
 * <p>Single source of truth for the chunk-size and key format used by every
 * writer (live ingestor archive sweep, batch backfill jobs) and reader
 * (cold-tier lookup) so they cannot drift.
 */
public final class ArchiveKey {

    public static final long CHUNK_SIZE = 1000;

    private ArchiveKey() {}

    /** Lower bound (inclusive) of the chunk that contains {@code blockNumber}. */
    public static long chunkStartFor(long blockNumber) {
        return (blockNumber / CHUNK_SIZE) * CHUNK_SIZE;
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
