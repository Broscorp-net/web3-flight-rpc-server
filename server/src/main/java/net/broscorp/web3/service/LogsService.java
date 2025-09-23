package net.broscorp.web3.service;

import io.reactivex.disposables.Disposable;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.subscription.LogSubscription;
import net.broscorp.web3.subscription.Subscription;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.websocket.WebSocketService;
import org.web3j.protocol.websocket.events.Notification;

import java.math.BigInteger;
import java.util.*;
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
    private final WebSocketService webSocketService;
    private final BigInteger maxBlockRange;
    private final List<Disposable> aggregatedSubscriptions = new ArrayList<>();

    private static class LogResponse extends Response<Log> { }
    private static class LogNotification extends Notification<Log> { }

    public LogsService(Web3j web3j, int maxBlockRange) {
        this(web3j, maxBlockRange, null, Executors.newVirtualThreadPerTaskExecutor());
    }

    public LogsService(Web3j web3j, int maxBlockRange, ExecutorService executor) {
        this(web3j, maxBlockRange, null, executor);
    }

    public LogsService(Web3j web3j, int maxBlockRange, WebSocketService webSocketService) {
        this(web3j, maxBlockRange, webSocketService, Executors.newVirtualThreadPerTaskExecutor());
    }

    public LogsService(Web3j web3j, int maxBlockRange, WebSocketService webSocketService, ExecutorService executor) {
        this.web3j = web3j;
        this.webSocketService = webSocketService;
        this.maxBlockRange = BigInteger.valueOf(maxBlockRange);
        this.workerExecutor = executor;
    }

    /**
     * Registers a new subscription for logs and starts processing it according to the associated request.
     */
    public void registerNewSubscription(LogSubscription subscription) {
        log.info("Log subscription registration for request: {}. Previously active: {}", subscription.getClientRequest(),
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
        aggregatedSubscriptions.forEach(Disposable::dispose);
        aggregatedSubscriptions.clear();

        if (subscriptions.isEmpty()) {
            log.info("No active subscriptions. Aggregated subscription not created.");
            return;
        }

        // Prefer true WebSocket subscriptions to avoid HTTP polling filters.
        if (webSocketService != null) {
            subscribeViaWebSocket();
            log.info("Aggregated WebSocket subscription created for {} subscriptions.", subscriptions.size());
            return;
        }

        // Fallback: Build filter and use ethLogFlowable (may use HTTP-style filters).
        EthFilter aggregatedFilter = buildRealtimeFilter();
        Disposable disposable = web3j.ethLogFlowable(aggregatedFilter)
                .subscribe(
                        this::onNewRealtimeLog,
                        err -> log.error("Realtime subscription error", err)
                );
        aggregatedSubscriptions.add(disposable);
        log.info("Aggregated WebSocket subscription created for {} subscriptions.", subscriptions.size());
    }

    private void subscribeViaWebSocket() {
        boolean subscribeAllAddresses = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> req.getContractAddresses() == null || req.getContractAddresses().isEmpty());

        boolean subscribeAllTopics = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> req.getTopics() == null || req.getTopics().isEmpty());

        List<String> allAddresses = subscriptions.stream()
                .map(sub -> sub.getClientRequest().getContractAddresses())
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();

        List<String> allTopics = subscriptions.stream()
                .map(sub -> sub.getClientRequest().getTopics())
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();

        Map<String, Object> filterParams = new LinkedHashMap<>();

        if (!subscribeAllAddresses && !allAddresses.isEmpty()) {
            filterParams.put("address", allAddresses);
        }

        // Topics: Ethereum allows up to 4 positions.
        // Put all event signatures into position 0.
        if (!subscribeAllTopics && !allTopics.isEmpty()) {
            filterParams.put("topics", List.of(allTopics));
        }

        Request<?, LogResponse> request = new Request<>(
                "eth_subscribe",
                List.of("logs", filterParams),
                webSocketService,
                LogResponse.class
        );

        Disposable disposable = webSocketService.subscribe(
                        request,
                        "eth_unsubscribe",
                        LogNotification.class
                )
                .map(notif -> {
                    Log logObj = notif.getParams().getResult();
                    log.debug("Realtime log received: block={}, tx={}, topics={}",
                            logObj.getBlockNumber(), logObj.getTransactionHash(), logObj.getTopics());
                    return logObj;
                })
                .subscribe(
                        this::onNewRealtimeLog,
                        err -> log.error("Realtime subscription error", err)
                );

        aggregatedSubscriptions.add(disposable);

        log.info("Created WebSocket subscription: {} addresses, {} topics (all in slot 0).",
                allAddresses.size(), allTopics.size());
    }


    private EthFilter buildRealtimeFilter() {
        boolean subscribeAllAddresses = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> req.getContractAddresses() == null || req.getContractAddresses().isEmpty());

        boolean subscribeAllTopics = subscriptions.stream()
                .map(Subscription::getClientRequest)
                .anyMatch(req -> req.getTopics() == null || req.getTopics().isEmpty());

        List<String> allAddresses = subscriptions.stream()
                .map(sub -> sub.getClientRequest().getContractAddresses())
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();

        List<String> allTopics = subscriptions.stream()
                .map(sub -> sub.getClientRequest().getTopics())
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .distinct()
                .toList();

        EthFilter filter;

        if (subscribeAllAddresses && subscribeAllTopics) {
            log.warn("Wildcard subscription active: all addresses & topics. Fetching ALL logs from node.");
            filter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, (List<String>) null);
        } else if (subscribeAllAddresses) {
            log.warn("Subscription to all addresses active. Filtering topics only.");
            filter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, (List<String>) null);
            filter.addOptionalTopics(allTopics.toArray(new String[0]));
        } else if (subscribeAllTopics) {
            log.warn("Subscription to all topics active. Filtering addresses only.");
            filter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, allAddresses);
        } else {
            log.info("Filtering by {} addresses and {} topics.", allAddresses.size(), allTopics.size());
            filter = new EthFilter(DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST, allAddresses);
            filter.addOptionalTopics(allTopics.toArray(new String[0]));
        }

        return filter;
    }

    private void onNewRealtimeLog(Log newLog) {
        List<Log> logBatch = List.of(newLog);
        for (LogSubscription sub : subscriptions) {
            sub.sendRealtime(logBatch);
        }
    }

    private void processNewSubscription(LogSubscription subscription) {
        LogsRequest request = subscription.getClientRequest();
        boolean awaitingForRealTimeData = request.awaitingForRealTimeData();

        if (awaitingForRealTimeData) {
            subscriptions.add(subscription);
            rebuildAggregatedWeb3jSubscription();
        }

        try {
            BigInteger endBlock = (request.getEndBlock() != null)
                    ? request.getEndBlock()
                    : web3j.ethBlockNumber().send().getBlockNumber();
            BigInteger startBlock = request.getStartBlock() != null ? request.getStartBlock() : endBlock;
            log.info("Last chain block: {}, starting from: {}", endBlock, startBlock);

            boolean canFetchHistoricalData = startBlock != null && startBlock.compareTo(endBlock) < 0;

            if (canFetchHistoricalData) {
                log.info("Fetching historical block range: {} - {}", startBlock, endBlock);
                BigInteger firstBatchBlock = startBlock;
                BigInteger lastBatchBlock = startBlock.add(maxBlockRange).subtract(BigInteger.ONE).compareTo(endBlock) <= 0
                        ? startBlock.add(maxBlockRange).subtract(BigInteger.ONE)
                        : endBlock;

                while (firstBatchBlock.compareTo(endBlock) <= 0) {
                    log.info("Fetching historical data for block range: {} - {}", firstBatchBlock, lastBatchBlock);
                    pushHistoricalData(subscription, firstBatchBlock, lastBatchBlock);
                    firstBatchBlock = lastBatchBlock.add(BigInteger.ONE);
                    lastBatchBlock = lastBatchBlock.add(maxBlockRange).subtract(BigInteger.ONE).compareTo(endBlock) <= 0
                            ? lastBatchBlock.add(maxBlockRange).subtract(BigInteger.ONE)
                            : endBlock;
                }
                subscription.completeBackfill();
            }

            if (!awaitingForRealTimeData) {
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