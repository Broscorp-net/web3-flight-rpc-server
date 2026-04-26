package net.broscorp.web3.backfill.source;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.backfill.ratelimit.TokenBucket;
import net.broscorp.web3.service.BlockchainProvider;
import net.broscorp.web3.service.BlockchainProvider.FullBlockData;
import net.broscorp.web3.service.Web3jBlockchainProvider;
import org.web3j.protocol.Web3j;
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
 */
@Slf4j
public class RpcBlockSource implements BlockSource {

    private static final int RPC_CALLS_PER_BLOCK = 3;

    private final HttpService httpService;
    private final Web3j web3j;
    private final BlockchainProvider provider;
    private final TokenBucket rateLimiter;

    public RpcBlockSource(String httpUrl, TokenBucket rateLimiter) {
        this.httpService = new HttpService(httpUrl);
        this.web3j = Web3j.build(httpService);
        this.provider = new Web3jBlockchainProvider(web3j, httpService);
        this.rateLimiter = rateLimiter;
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
        return provider.fetchFullBlock(BigInteger.valueOf(blockNumber));
    }

    @Override
    public void close() {
        web3j.shutdown();
    }
}
