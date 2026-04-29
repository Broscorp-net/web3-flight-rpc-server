package net.broscorp.web3.backfill.source;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.backfill.ratelimit.TokenBucket;
import net.broscorp.web3.service.BlockchainProvider;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import net.broscorp.web3.service.JsonRpcException;
import net.broscorp.web3.service.MalformedRpcResponseException;
import net.broscorp.web3.service.Web3jBlockchainProvider;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.exceptions.ClientConnectionException;
import org.web3j.protocol.http.HttpService;

/**
 * {@link BlockSource} backed by the same {@link Web3jBlockchainProvider} the
 * live ingestor uses for forward fetches. Built on top of {@code eth_getBlockByNumber},
 * {@code eth_getLogs} and {@code eth_getBlockReceipts}.
 *
 * <p>If a {@link TokenBucket} is provided, {@link #fetchBlock} blocks the
 * caller until 3 tokens are available (one per RPC call the provider issues
 * per block). The producer thread in the orchestrator is the natural caller,
 * so blocking here throttles new fetches without affecting in-flight ones.
 *
 * <p>Transient upstream failures (HTTP 429, 5xx, network resets, JSON-RPC
 * rate-limit error codes) are retried with exponential backoff + jitter
 * inside this source. Non-transient errors (4xx other than 429, malformed
 * responses, missing-block errors, receipts/transactions-count mismatch from
 * {@link Web3jBlockchainProvider}) fail fast.
 */
@Slf4j
public class RpcBlockSource implements BlockSource {

    private static final int RPC_CALLS_PER_BLOCK = 3;

    /** Default policy: 6 total attempts, 500ms base, 30s cap. */
    public static final RetryPolicy DEFAULT_RETRY_POLICY =
        new RetryPolicy(6, 500L, 30_000L);

    private static final Pattern HTTP_STATUS_IN_MSG =
        Pattern.compile("Invalid response received:\\s*(\\d{3})");

    private final HttpService httpService;
    private final Web3j web3j;
    private final BlockchainProvider provider;
    private final TokenBucket rateLimiter;
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService scheduler;

    public RpcBlockSource(String httpUrl, TokenBucket rateLimiter) {
        this(httpUrl, rateLimiter, DEFAULT_RETRY_POLICY);
    }

    public RpcBlockSource(
        String httpUrl,
        TokenBucket rateLimiter,
        RetryPolicy retryPolicy
    ) {
        this.httpService = new HttpService(httpUrl);
        this.web3j = Web3j.build(httpService);
        this.provider = new Web3jBlockchainProvider(web3j, httpService);
        this.rateLimiter = rateLimiter;
        this.retryPolicy = retryPolicy;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rpc-retry-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /** Visible for testing. */
    RpcBlockSource(
        BlockchainProvider provider,
        TokenBucket rateLimiter,
        RetryPolicy retryPolicy,
        ScheduledExecutorService scheduler
    ) {
        this.httpService = null;
        this.web3j = null;
        this.provider = provider;
        this.rateLimiter = rateLimiter;
        this.retryPolicy = retryPolicy;
        this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<FullBlockData> fetchBlock(long blockNumber) {
        if (rateLimiter != null) {
            try {
                rateLimiter.acquire(RPC_CALLS_PER_BLOCK);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.failedFuture(e);
            }
        }
        return fetchWithRetry(blockNumber, 1);
    }

    private CompletableFuture<FullBlockData> fetchWithRetry(
        long blockNumber,
        int attempt
    ) {
        CompletableFuture<FullBlockData> result = new CompletableFuture<>();
        provider
            .fetchFullBlock(BigInteger.valueOf(blockNumber))
            .whenComplete((data, err) -> {
                if (err == null) {
                    result.complete(data);
                    return;
                }
                Throwable cause = unwrap(err);
                if (
                    !isTransient(cause) || attempt >= retryPolicy.maxAttempts()
                ) {
                    result.completeExceptionally(cause);
                    return;
                }
                long delay = backoffMs(attempt);
                log.warn(
                    "Block {} fetch attempt {}/{} failed transiently ({}); retrying in {}ms",
                    blockNumber,
                    attempt,
                    retryPolicy.maxAttempts(),
                    cause.toString(),
                    delay
                );
                scheduler.schedule(
                    () ->
                        fetchWithRetry(blockNumber, attempt + 1)
                            .whenComplete((d, e) -> {
                                if (e == null) result.complete(d);
                                else result.completeExceptionally(unwrap(e));
                            }),
                    delay,
                    TimeUnit.MILLISECONDS
                );
            });
        return result;
    }

    private long backoffMs(int attempt) {
        // attempt is 1-based; first retry uses base delay.
        int shift = Math.min(attempt - 1, 16);
        long base = retryPolicy.baseDelayMs() << shift;
        if (base <= 0 || base > retryPolicy.maxDelayMs()) {
            base = retryPolicy.maxDelayMs();
        }
        long jitterRange = Math.max(1L, base / 5);
        long jitter = ThreadLocalRandom.current().nextLong(
            -jitterRange, jitterRange + 1
        );
        return Math.max(0L, base + jitter);
    }

    static Throwable unwrap(Throwable t) {
        while (
            (t instanceof CompletionException || t instanceof ExecutionException)
            && t.getCause() != null
            && t.getCause() != t
        ) {
            t = t.getCause();
        }
        return t;
    }

    static boolean isTransient(Throwable cause) {
        // Structurally-valid response whose payload is incoherent — empirically
        // the upstream node truncated/dropped the body under load. Retry.
        if (cause instanceof MalformedRpcResponseException) return true;
        if (cause instanceof JsonRpcException jre) {
            int code = jre.code();
            // -32005: Alchemy/Infura "limit exceeded".
            // 429: some nodes surface HTTP status in the JSON-RPC error code.
            if (code == -32005 || code == 429) return true;
            // -32603 (internal) sometimes wraps overload/timeout messages.
            if (code == -32603 && cause.getMessage() != null) {
                String m = cause.getMessage().toLowerCase();
                return m.contains("rate")
                    || m.contains("limit")
                    || m.contains("overload")
                    || m.contains("timeout")
                    || m.contains("busy");
            }
            return false;
        }
        if (cause instanceof ClientConnectionException) {
            String msg = cause.getMessage();
            if (msg != null) {
                Matcher m = HTTP_STATUS_IN_MSG.matcher(msg);
                if (m.find()) {
                    int status = Integer.parseInt(m.group(1));
                    return status == 429 || (status >= 500 && status < 600);
                }
            }
            // No parseable status — treat as transport glitch.
            return true;
        }
        // SocketTimeoutException, ConnectException, EOFException, etc.
        return cause instanceof IOException;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        if (web3j != null) {
            web3j.shutdown();
        }
    }

    public record RetryPolicy(
        int maxAttempts,
        long baseDelayMs,
        long maxDelayMs
    ) {
        public RetryPolicy {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            if (baseDelayMs < 0 || maxDelayMs < baseDelayMs) {
                throw new IllegalArgumentException(
                    "invalid delay range: base=" + baseDelayMs
                        + " max=" + maxDelayMs
                );
            }
        }
    }
}
