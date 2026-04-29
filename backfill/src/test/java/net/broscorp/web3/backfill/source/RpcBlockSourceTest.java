package net.broscorp.web3.backfill.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.broscorp.web3.backfill.source.RpcBlockSource.RetryPolicy;
import net.broscorp.web3.service.BlockchainProvider;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import net.broscorp.web3.service.JsonRpcException;
import net.broscorp.web3.service.MalformedRpcResponseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.exceptions.ClientConnectionException;

class RpcBlockSourceTest {

    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void isTransient_classifiesJsonRpcRateLimitCodes() {
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32005, "limit exceeded")
            )
        ).isTrue();
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", 429, "Too Many Requests")
            )
        ).isTrue();
    }

    @Test
    void isTransient_treats32603OnlyAsTransientWhenMessageIndicatesOverload() {
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32603, "rate limit hit")
            )
        ).isTrue();
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32603, "node is busy")
            )
        ).isTrue();
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32603, "internal logic error")
            )
        ).isFalse();
    }

    @Test
    void isTransient_classifiesPermanentJsonRpcCodesAsNonTransient() {
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32602, "invalid params")
            )
        ).isFalse();
        assertThat(
            RpcBlockSource.isTransient(
                new JsonRpcException("eth_getLogs", -32601, "method not found")
            )
        ).isFalse();
    }

    @Test
    void isTransient_classifiesHttp429And5xxFromConnectionExceptionMessage() {
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException(
                    "Invalid response received: 429; body=Too Many Requests"
                )
            )
        ).isTrue();
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException(
                    "Invalid response received: 503; body=Service Unavailable"
                )
            )
        ).isTrue();
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException(
                    "Invalid response received: 500; body=Internal Error"
                )
            )
        ).isTrue();
    }

    @Test
    void isTransient_classifiesNon429Or5xxClientErrorsAsNonTransient() {
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException(
                    "Invalid response received: 400; body=Bad Request"
                )
            )
        ).isFalse();
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException(
                    "Invalid response received: 401; body=Unauthorized"
                )
            )
        ).isFalse();
    }

    @Test
    void isTransient_treatsClientConnectionExceptionWithoutStatusAsTransient() {
        assertThat(
            RpcBlockSource.isTransient(
                new ClientConnectionException("connection closed mid-stream")
            )
        ).isTrue();
    }

    @Test
    void isTransient_classifiesNetworkIoExceptionsAsTransient() {
        assertThat(RpcBlockSource.isTransient(new SocketTimeoutException()))
            .isTrue();
        assertThat(RpcBlockSource.isTransient(new EOFException()))
            .isTrue();
        assertThat(RpcBlockSource.isTransient(new IOException("conn reset")))
            .isTrue();
    }

    @Test
    void isTransient_classifiesMalformedRpcResponseAsTransient() {
        // Real-world case 2026-04-29: block reported 139 txs, upstream returned
        // 0 receipts. Not a permanent error — the next attempt typically wins.
        assertThat(
            RpcBlockSource.isTransient(
                new MalformedRpcResponseException(
                    "Block 1 receipts/transactions mismatch: ..."
                )
            )
        ).isTrue();
    }

    @Test
    void isTransient_doesNotRetryProgrammingErrors() {
        assertThat(
            RpcBlockSource.isTransient(
                new IllegalArgumentException("invalid argument")
            )
        ).isFalse();
        assertThat(
            RpcBlockSource.isTransient(
                new NullPointerException("npe in caller")
            )
        ).isFalse();
    }

    @Test
    void unwrap_stripsCompletionAndExecutionException() {
        IOException root = new IOException("net");
        assertThat(
            RpcBlockSource.unwrap(new CompletionException(root))
        ).isSameAs(root);
        assertThat(
            RpcBlockSource.unwrap(
                new CompletionException(new ExecutionException(root))
            )
        ).isSameAs(root);
    }

    @Test
    void retry_succeedsAfterTransientFailures() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        FullBlockData expected = new FullBlockData(
            null, Collections.emptyList(), Collections.emptyMap()
        );
        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.ZERO);
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                int n = calls.incrementAndGet();
                if (n < 3) {
                    return CompletableFuture.failedFuture(
                        new JsonRpcException("eth_getLogs", -32005, "limit exceeded")
                    );
                }
                return CompletableFuture.completedFuture(expected);
            }
        };

        try (
            RpcBlockSource source = new RpcBlockSource(
                provider,
                null,
                new RetryPolicy(5, 1L, 5L),
                scheduler
            )
        ) {
            FullBlockData got = source.fetchBlock(42L).get(2, TimeUnit.SECONDS);
            assertThat(got).isSameAs(expected);
            assertThat(calls.get()).isEqualTo(3);
        }
    }

    @Test
    void retry_givesUpAfterMaxAttempts() {
        AtomicInteger calls = new AtomicInteger();
        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.ZERO);
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                calls.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new SocketTimeoutException("read timed out")
                );
            }
        };

        try (
            RpcBlockSource source = new RpcBlockSource(
                provider,
                null,
                new RetryPolicy(4, 1L, 5L),
                scheduler
            )
        ) {
            assertThatThrownBy(() ->
                source.fetchBlock(42L).get(2, TimeUnit.SECONDS)
            ).hasRootCauseInstanceOf(SocketTimeoutException.class);
            assertThat(calls.get()).isEqualTo(4);
        }
    }

    @Test
    void retry_doesNotRetryNonTransientErrors() {
        AtomicInteger calls = new AtomicInteger();
        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.ZERO);
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                calls.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("bad argument")
                );
            }
        };

        try (
            RpcBlockSource source = new RpcBlockSource(
                provider,
                null,
                new RetryPolicy(6, 1L, 5L),
                scheduler
            )
        ) {
            assertThatThrownBy(() ->
                source.fetchBlock(42L).get(2, TimeUnit.SECONDS)
            ).hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void retry_recoversFromReceiptsCountMismatch() throws Exception {
        // The exact case that fired in prod 2026-04-29 — the first attempt
        // hits an upstream node returning truncated receipts, the second
        // succeeds.
        AtomicInteger calls = new AtomicInteger();
        FullBlockData expected = new FullBlockData(
            null, Collections.emptyList(), Collections.emptyMap()
        );
        BlockchainProvider provider = new BlockchainProvider() {
            @Override
            public CompletableFuture<BigInteger> getLatestBlockNumber() {
                return CompletableFuture.completedFuture(BigInteger.ZERO);
            }

            @Override
            public CompletableFuture<FullBlockData> fetchFullBlock(
                BigInteger blockNumber
            ) {
                int n = calls.incrementAndGet();
                if (n == 1) {
                    return CompletableFuture.failedFuture(
                        new MalformedRpcResponseException(
                            "Block " + blockNumber
                                + " receipts/transactions mismatch:"
                                + " block has 139 txs but received 0 receipts"
                        )
                    );
                }
                return CompletableFuture.completedFuture(expected);
            }
        };

        try (
            RpcBlockSource source = new RpcBlockSource(
                provider,
                null,
                new RetryPolicy(4, 1L, 5L),
                scheduler
            )
        ) {
            FullBlockData got = source.fetchBlock(45198695L)
                .get(2, TimeUnit.SECONDS);
            assertThat(got).isSameAs(expected);
            assertThat(calls.get()).isEqualTo(2);
        }
    }
}
