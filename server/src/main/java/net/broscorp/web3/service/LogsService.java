package net.broscorp.web3.service;

import io.reactivex.disposables.Disposable;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.subscription.LogSubscription;
import net.broscorp.web3.subscription.Subscription;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A service that handles Subscriptions to Logs on the Ethereum blockchain.
 * <p>
 * Encapsulates the logic of handling the list of subscriptions as well as managing interactions with the blockchain through web3j dependencies.
 * <p>
 * Supports Requests for historical and real-time data, attempts a seamless switch between historical and real-time data.
 */
@Slf4j
public class LogsService {

    private final Web3j web3j;
    private final ExecutorService workerExecutor;
    private final List<LogSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final BigInteger maxBlockRange;
    private Disposable aggregatedSubscription;

    public LogsService(Web3j web3j, int maxBlockRange) {
        this.web3j = web3j;
        this.maxBlockRange = BigInteger.valueOf(maxBlockRange);
        this.workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    LogsService(Web3j web3j, int maxBlockRange, ExecutorService executor) {
        this.web3j = web3j;
        this.maxBlockRange = BigInteger.valueOf(maxBlockRange);
        this.workerExecutor = executor;
    }

    /**
     * Registers a new subscription for logs and starts processing it according to the associated request.
     */
    public void registerNewSubscription(LogSubscription subscription) {
        log.info("Log subscription registration for request: {}. Total active: {}", subscription.getClientRequest(),
                subscriptions.size());

        subscription.setOnCancelHandler(() -> handleSubscriptionRemoval(subscription));
        subscription.setOnErrorHandler(() -> handleSubscriptionRemoval(subscription));
        workerExecutor.submit(() -> processNewSubscription(subscription));
    }

    private void handleSubscriptionRemoval(LogSubscription subscription) {
        log.info("Removing client subscription for logs: {}", subscription.getClientRequest());
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
                log.info("No active log listeners. Tearing down main aggregated subscription.");
                aggregatedSubscription.dispose();
                aggregatedSubscription = null;
            }
            return;
        }

        // Determine if we need to subscribe to all logs.
        boolean shouldSubscribeToAllAddresses = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> (req.getContractAddresses() == null || req.getContractAddresses().isEmpty()));
        boolean shouldSubscribeToAllTopics = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> (req.getTopics() == null || req.getTopics().isEmpty()));

        EthFilter aggregatedFilter;

        if (shouldSubscribeToAllAddresses && shouldSubscribeToAllTopics) {
            log.warn("A wildcard subscription is active. Fetching ALL logs from node provider.");
            aggregatedFilter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, (List<String>) null);
        } else if (shouldSubscribeToAllTopics) {
            log.warn("A subscription to all topics is active.");
            List<String> allAddresses = subscriptions.stream()
                    .map(sub -> sub.getClientRequest().getContractAddresses())
                    .filter(java.util.Objects::nonNull)
                    .flatMap(Collection::stream)
                    .distinct()
                    .toList();
            log.info("All subscriptions are address-specific. Rebuilding aggregated log filter for {} addresses.", allAddresses.size());

            aggregatedFilter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, allAddresses);
        } else if (shouldSubscribeToAllAddresses) {
            log.warn("A subscription to all addresses is active.");
            String[] specifiedTopics = subscriptions.stream()
                    .map(sub -> sub.getClientRequest().getTopics())
                    .filter(java.util.Objects::nonNull)
                    .flatMap(Collection::stream)
                    .distinct()
                    .toArray(String[]::new);
            log.info("All subscriptions are topics-specific. Rebuilding aggregated log filter for {} topics.", specifiedTopics.length);

            aggregatedFilter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, (List<String>) null);
            aggregatedFilter.addOptionalTopics(specifiedTopics);
        } else {
            List<String> allAddresses = subscriptions.stream()
                    .map(sub -> sub.getClientRequest().getContractAddresses())
                    .filter(java.util.Objects::nonNull)
                    .flatMap(Collection::stream)
                    .distinct()
                    .toList();

            String[] specifiedTopics = subscriptions.stream()
                    .map(sub -> sub.getClientRequest().getTopics())
                    .filter(java.util.Objects::nonNull)
                    .flatMap(Collection::stream)
                    .distinct()
                    .toArray(String[]::new);

            log.info("All subscriptions are topics and address-specific. Rebuilding aggregated log filter for {} addresses, {} topics.", allAddresses.size(), specifiedTopics.length);
            aggregatedFilter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, allAddresses);
            aggregatedFilter.addOptionalTopics(specifiedTopics);
        }

        if (aggregatedSubscription != null) {
            aggregatedSubscription.dispose();
        }

        aggregatedSubscription = web3j.ethLogFlowable(aggregatedFilter)
                .subscribe(this::onNewRealtimeLog, err -> log.error("Aggregated log subscription error", err));
    }

    private void onNewRealtimeLog(Log newLog) {
        List<Log> logBatch = List.of(newLog);
        for (LogSubscription sub : subscriptions) {
            sub.sendRealtime(logBatch);
        }
    }

    private void processNewSubscription(LogSubscription subscription) {
        LogsRequest request = subscription.getClientRequest();
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

            boolean canFetchHistoricalData = startBlock != null && startBlock.compareTo(endBlock) <= 0;

            if (canFetchHistoricalData) {
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
                log.info("Finished historical request for logs. {}", request);
                subscription.close();
            }
        } catch (Exception e) {
            log.error("Error during historical backfill", e);
            subscription.error(e);
            handleSubscriptionRemoval(subscription);
        }
    }

    private void pushHistoricalData(LogSubscription subscription, BigInteger startBlock, BigInteger endBlock) throws Exception {
        LogsRequest request = subscription.getClientRequest();
        log.info("Pushing historical data for log subscription: {}, startBlock {}, endBlock {}",
                subscription.getClientRequest(),
                startBlock,
                endBlock);
        EthFilter historicalFilter = new EthFilter(
                DefaultBlockParameter.valueOf(startBlock),
                DefaultBlockParameter.valueOf(endBlock),
                Optional.ofNullable(request.getContractAddresses()).orElse(List.of())
        );

        if (!Objects.isNull(request.getTopics()) && !request.getTopics().isEmpty())
            historicalFilter.addOptionalTopics(request.getTopics().toArray(new String[0]));

        EthLog ethLog = web3j.ethGetLogs(historicalFilter).send();

        if (ethLog.hasError() || ethLog.getLogs() == null) {
            String errorMessage = ethLog.hasError() ? ethLog.getError().getMessage() : "Node returned a null result for logs query.";
            log.error("Failed to get historical logs from node: {}", errorMessage);
            throw new RuntimeException("Failed to fetch historical logs: " + errorMessage);
        }

        List<Log> historicalLogs = ethLog.getLogs().stream()
                .map(logResult -> (Log) logResult.get()).collect(Collectors.toList());

        subscription.sendHistorical(historicalLogs);
        log.info("Finished historical backfill for client. Sent {} logs.", historicalLogs.size());
    }
}