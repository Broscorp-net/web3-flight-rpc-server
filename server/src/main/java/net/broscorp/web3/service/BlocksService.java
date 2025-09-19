package net.broscorp.web3.service;

import io.reactivex.disposables.Disposable;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.subscription.BlockSubscription;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A service that handles Subscriptions to Blocks on the Ethereum blockchain.
 * <p>
 * Encapsulates the logic of handling the list of subscriptions as well as managing interactions with the blockchain through web3j dependencies.
 * <p>
 * Supports Requests for historical and real-time data, attempts a seamless switch between historical and real-time data.
 */
@Slf4j
public class BlocksService {

    private final Web3j web3j;
    private final ExecutorService workerExecutor;
    private final List<BlockSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final BigInteger maxBlockRange;
    private Disposable aggregatedSubscription;

    public BlocksService(Web3j web3j, int maxBlockRange) {
        this.web3j = web3j;
        this.maxBlockRange = BigInteger.valueOf(maxBlockRange);
        this.workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    BlocksService(Web3j web3j, int maxBlockRange, ExecutorService executor) {
        this.web3j = web3j;
        this.maxBlockRange = BigInteger.valueOf(maxBlockRange);
        this.workerExecutor = executor;
    }

    /**
     * Registers a new subscription for blocks (without full transaction objects)
     * and starts processing it according to the associated request.
     */
    public void registerNewSubscription(BlockSubscription subscription) {
        log.info("Block subscription registration for request: {}. Total active: {}", subscription.getClientRequest(),
                subscriptions.size());

        subscription.setOnCancelHandler(() -> handleSubscriptionRemoval(subscription));
        workerExecutor.submit(() -> processNewSubscription(subscription));
    }

    private void handleSubscriptionRemoval(BlockSubscription subscription) {
        log.info("Removing client subscription for blocks: {}", subscription.getClientRequest());
        try {
            subscriptions.remove(subscription);
            subscription.close();
        } catch (Exception e) {
            log.error("Error closing subscription resources.", e);
            throw new RuntimeException(e);
        } finally {
            rebuildAggregatedWeb3jSubscription();
        }
    }

    private synchronized void rebuildAggregatedWeb3jSubscription() {
        if (subscriptions.isEmpty()) {
            if (aggregatedSubscription != null) {
                log.info("No active block listeners. Tearing down main subscription.");
                aggregatedSubscription.dispose();
                aggregatedSubscription = null;
            }
            return;
        }

        aggregatedSubscription = web3j.blockFlowable(false)
                .subscribe(this::onNewRealtimeBlock, err -> log.error("Aggregated block subscription error", err));
    }

    private void onNewRealtimeBlock(EthBlock newBlock) {
        List<EthBlock.Block> blockBatch = List.of(newBlock.getBlock());
        for (BlockSubscription sub : subscriptions) {
            sub.sendRealtime(blockBatch);
        }
    }

    private void processNewSubscription(BlockSubscription subscription) {
        BlocksRequest request = subscription.getClientRequest();
        boolean isRealtime = request.isRealtime();

        if (isRealtime) {
            subscriptions.add(subscription);
            rebuildAggregatedWeb3jSubscription();
        }

        try {
            BigInteger startBlock = request.getStartBlock();
            BigInteger endBlock = (request.getEndBlock() != null)
                    ? request.getEndBlock()
                    : web3j.ethBlockNumber().send().getBlockNumber();

            boolean needsHistoricalData = startBlock != null && startBlock.compareTo(endBlock) < 0;

            if (needsHistoricalData) {
                BigInteger firstBatchBlock = startBlock;
                BigInteger lastBatchBlock = startBlock.add(maxBlockRange).compareTo(endBlock) <= 0
                        ? startBlock.add(maxBlockRange)
                        : endBlock;

                while (firstBatchBlock.compareTo(endBlock) <= 0) {
                    pushHistoricalData(subscription, firstBatchBlock, lastBatchBlock);
                    firstBatchBlock = lastBatchBlock.add(BigInteger.ONE);
                    lastBatchBlock = lastBatchBlock.add(maxBlockRange).compareTo(endBlock) <= 0
                            ? lastBatchBlock.add(maxBlockRange)
                            : endBlock;
                }
                subscription.completeBackfill();
            }

            if (!isRealtime) {
                log.info("Finished historical request for blocks. {}", request);
                subscription.close();
            }
        } catch (Exception e) {
            log.error("Error during historical backfill", e);
            subscription.error(e);
            handleSubscriptionRemoval(subscription);
        }
    }

    private void pushHistoricalData(BlockSubscription subscription, BigInteger startBlock, BigInteger endBlock) throws Exception {
        log.info("Pushing historical data for block subscription: {}, startBlock {}, endBlock {}",
                subscription.getClientRequest(),
                startBlock,
                endBlock);

        List<EthBlock.Block> requestResults = new ArrayList<>(endBlock.subtract(startBlock).add(BigInteger.ONE).intValue());

        for (BigInteger i = startBlock; i.compareTo(endBlock) <= 0; i = i.add(BigInteger.ONE)) {
            EthBlock block = web3j.ethGetBlockByNumber(
                    new DefaultBlockParameterNumber(i), false
            ).send();
            requestResults.add(block.getBlock());
        }

        subscription.sendHistorical(requestResults);
        log.info("Finished historical backfill for client. Sent {} blocks.", requestResults.size());
    }
}
