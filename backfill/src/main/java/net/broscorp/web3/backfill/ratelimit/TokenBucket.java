package net.broscorp.web3.backfill.ratelimit;

/**
 * Simple token-bucket rate limiter. {@link #acquire(int)} blocks until the
 * requested number of tokens are available, refilling continuously at the
 * configured rate.
 *
 * <p>Designed for the backfill's HTTP gate: a single instance shared across
 * the producer thread, called once per outgoing RPC fetch. Not a free-for-all
 * concurrency primitive — uses {@code synchronized} on itself, so contention
 * scales linearly with the number of acquirers; fine for a single-producer
 * orchestrator, not fine for hundreds of threads.
 */
public final class TokenBucket {

    private final double maxTokens;
    private final double refillPerNs;
    private double tokens;
    private long lastRefillNs;

    public TokenBucket(double tokensPerSecond, double burstCapacity) {
        if (tokensPerSecond <= 0) {
            throw new IllegalArgumentException(
                "tokensPerSecond must be > 0, got " + tokensPerSecond
            );
        }
        if (burstCapacity < 1) {
            throw new IllegalArgumentException(
                "burstCapacity must be >= 1, got " + burstCapacity
            );
        }
        this.maxTokens = burstCapacity;
        this.refillPerNs = tokensPerSecond / 1_000_000_000.0;
        this.tokens = burstCapacity;
        this.lastRefillNs = System.nanoTime();
    }

    public synchronized void acquire(int permits) throws InterruptedException {
        if (permits <= 0) return;
        if (permits > maxTokens) {
            throw new IllegalArgumentException(
                "permits " + permits + " exceeds burst capacity " + maxTokens
            );
        }
        while (true) {
            long now = System.nanoTime();
            tokens = Math.min(
                maxTokens,
                tokens + (now - lastRefillNs) * refillPerNs
            );
            lastRefillNs = now;
            if (tokens >= permits) {
                tokens -= permits;
                return;
            }
            double need = permits - tokens;
            long waitNs = (long) Math.ceil(need / refillPerNs);
            long waitMs = waitNs / 1_000_000L;
            int waitNanos = (int) (waitNs % 1_000_000L);
            wait(Math.max(1L, waitMs), waitNanos);
        }
    }
}
