package net.broscorp.web3.service;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.metrics.Metrics;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Durable cache for per-block Arrow IPC bytes backed by RocksDB.
 *
 * Key layout:
 *   'b' + BE(long) -> block header IPC bytes
 *   'l' + BE(long) -> block logs   IPC bytes
 *   META/lastBlock -> BE(long) highest contiguously-committed block number
 *   META/pruneFloor -> BE(long) highest block that has been pruned (exclusive floor)
 *
 * Concurrency invariant: {@link #commit(long, byte[], byte[])} is called by a single
 * writer (the ingestor's committer thread), so {@code lastIngestedBlock} is strictly
 * monotonic. Any number of readers may call the get/wait methods concurrently.
 */
@Slf4j
public class BlockchainCache implements AutoCloseable {

    public enum Status { LOADED, PRUNED, BACKFILLING }

    public record CacheResult(Status status, byte[] data) {
        public static CacheResult loaded(byte[] data) {
            return new CacheResult(Status.LOADED, data);
        }
        public static CacheResult pruned() {
            return new CacheResult(Status.PRUNED, null);
        }
        public static CacheResult backfilling() {
            return new CacheResult(Status.BACKFILLING, null);
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    private static final byte[] META_LAST_BLOCK =
        "META/lastBlock".getBytes();
    private static final byte[] META_PRUNE_FLOOR =
        "META/pruneFloor".getBytes();
    private static final byte[] META_FORWARD_START =
        "META/forwardStart".getBytes();

    private final RocksDB db;
    private final Metrics metrics;
    private final Lock lock = new ReentrantLock();
    private final Condition blockAvailable = lock.newCondition();
    private volatile long lastIngestedBlock = -1;
    private volatile long pruneFloor = 0;
    private volatile long forwardStart = 0;

    public BlockchainCache(String dbPath, Metrics metrics) throws RocksDBException {
        File dir = new File(dbPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        Options options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options, dbPath);
        this.metrics = metrics;
        this.lastIngestedBlock = readMetaLong(META_LAST_BLOCK, -1L);
        this.pruneFloor = readMetaLong(META_PRUNE_FLOOR, 0L);
        this.forwardStart = readMetaLong(META_FORWARD_START, this.pruneFloor);
        metrics.ingestorCommittedBlock.set(lastIngestedBlock);
        metrics.cachePruneFloor.set(pruneFloor);
        log.info(
            "Cache initialized: lastIngestedBlock={}, pruneFloor={}, forwardStart={}",
            lastIngestedBlock,
            pruneFloor,
            forwardStart
        );
    }

    /**
     * Atomically writes block header and logs IPC bytes for a single block and
     * advances {@code lastIngestedBlock}. Must only be called by the committer
     * thread with strictly-increasing block numbers.
     */
    public void commit(long blockNumber, byte[] blockIpc, byte[] logsIpc)
        throws RocksDBException {
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            batch.put(makeKey('b', blockNumber), blockIpc);
            batch.put(makeKey('l', blockNumber), logsIpc);
            batch.put(META_LAST_BLOCK, encodeLong(blockNumber));
            db.write(opts, batch);
        }
        lock.lock();
        try {
            lastIngestedBlock = blockNumber;
            blockAvailable.signalAll();
        } finally {
            lock.unlock();
        }
        metrics.ingestorBlocksCommittedTotal.inc();
        metrics.ingestorCommittedBlock.set(blockNumber);
    }

    /**
     * Deletes all blocks and logs with block number strictly less than
     * {@code beforeBlock} and advances the prune floor.
     */
    public void prune(long beforeBlock) throws RocksDBException {
        if (beforeBlock <= pruneFloor) return;

        log.info("Pruning cache: removing data before block {}", beforeBlock);

        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            batch.deleteRange(makeKey('b', 0), makeKey('b', beforeBlock));
            batch.deleteRange(makeKey('l', 0), makeKey('l', beforeBlock));
            batch.put(META_PRUNE_FLOOR, encodeLong(beforeBlock));
            db.write(opts, batch);
        }
        pruneFloor = beforeBlock;
        metrics.cachePruneFloor.set(beforeBlock);
    }

    /**
     * Sets the forward-ingestion start boundary. Blocks in
     * {@code [pruneFloor, forwardStart)} that are not yet present are reported
     * as {@link Status#BACKFILLING} (not waited on); blocks {@code >=
     * forwardStart} fall under the regular forward-commit invariant.
     */
    public void setForwardStart(long block) throws RocksDBException {
        try (WriteOptions opts = new WriteOptions()) {
            db.put(opts, META_FORWARD_START, encodeLong(block));
        }
        forwardStart = block;
    }

    /**
     * Writes a single backfilled block out-of-order without touching
     * {@code lastIngestedBlock}. Caller must ensure {@code pruneFloor <=
     * blockNumber < forwardStart}.
     */
    public void commitBackfill(long blockNumber, byte[] blockIpc, byte[] logsIpc)
        throws RocksDBException {
        try (WriteBatch batch = new WriteBatch(); WriteOptions opts = new WriteOptions()) {
            batch.put(makeKey('b', blockNumber), blockIpc);
            batch.put(makeKey('l', blockNumber), logsIpc);
            db.write(opts, batch);
        }
        metrics.ingestorBlocksCommittedTotal.inc();
    }

    public Optional<byte[]> getBlock(long blockNumber) throws RocksDBException {
        return Optional.ofNullable(db.get(makeKey('b', blockNumber)));
    }

    public Optional<byte[]> getLogs(long blockNumber) throws RocksDBException {
        return Optional.ofNullable(db.get(makeKey('l', blockNumber)));
    }

    /**
     * Returns the IPC bytes for the given block header, blocking if the block
     * has not yet been ingested. Returns {@link Status#PRUNED} if the block
     * is below the prune floor (caller should consult the cold archive).
     */
    public CacheResult getBlockOrWait(long blockNumber)
        throws RocksDBException, InterruptedException {
        return getOrWait(blockNumber, 'b');
    }

    public CacheResult getLogsOrWait(long blockNumber)
        throws RocksDBException, InterruptedException {
        return getOrWait(blockNumber, 'l');
    }

    private CacheResult getOrWait(long blockNumber, char type)
        throws RocksDBException, InterruptedException {
        String dataset = datasetLabel(type);
        if (blockNumber < pruneFloor) {
            metrics.cacheReadsTotal.labels(dataset, "pruned").inc();
            return CacheResult.pruned();
        }

        byte[] data = db.get(makeKey(type, blockNumber));
        if (data != null) {
            metrics.cacheReadsTotal.labels(dataset, "loaded").inc();
            return CacheResult.loaded(data);
        }

        if (blockNumber < forwardStart) {
            metrics.cacheReadsTotal.labels(dataset, "backfilling").inc();
            return CacheResult.backfilling();
        }

        Histogram.Timer waitTimer =
            metrics.cacheWaitDurationSeconds.labels(dataset).startTimer();
        try {
            lock.lock();
            try {
                while (blockNumber > lastIngestedBlock) {
                    if (blockNumber < pruneFloor) {
                        metrics.cacheReadsTotal.labels(dataset, "pruned").inc();
                        return CacheResult.pruned();
                    }
                    blockAvailable.await();
                }
                data = db.get(makeKey(type, blockNumber));
                if (data != null) {
                    metrics.cacheReadsTotal.labels(dataset, "waited").inc();
                    return CacheResult.loaded(data);
                }
                if (blockNumber < pruneFloor) {
                    metrics.cacheReadsTotal.labels(dataset, "pruned").inc();
                    return CacheResult.pruned();
                }
                throw new IllegalStateException(
                    "Block " + blockNumber + " missing from cache (type=" + type +
                    "); lastIngested=" + lastIngestedBlock +
                    ", pruneFloor=" + pruneFloor +
                    ", forwardStart=" + forwardStart
                );
            } finally {
                lock.unlock();
            }
        } finally {
            waitTimer.observeDuration();
        }
    }

    private static String datasetLabel(char type) {
        return type == 'b' ? "blocks" : "logs";
    }

    public long getLastIngestedBlock() {
        return lastIngestedBlock;
    }

    public long getPruneFloor() {
        return pruneFloor;
    }

    public long getForwardStart() {
        return forwardStart;
    }

    private long readMetaLong(byte[] key, long defaultValue)
        throws RocksDBException {
        byte[] raw = db.get(key);
        if (raw == null || raw.length != 8) return defaultValue;
        return ByteBuffer.wrap(raw).getLong();
    }

    private static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static byte[] makeKey(char type, long blockNumber) {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put((byte) type);
        buffer.putLong(blockNumber);
        return buffer.array();
    }

    @Override
    public void close() {
        db.close();
    }
}
