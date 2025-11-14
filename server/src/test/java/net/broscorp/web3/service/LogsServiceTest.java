package net.broscorp.web3.service;

import io.reactivex.Flowable;
import lombok.SneakyThrows;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.subscription.LogSubscription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.request.Filter;
import org.web3j.protocol.core.methods.response.EthBlockNumber;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.exceptions.ClientConnectionException;
import org.web3j.protocol.websocket.WebSocketService;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogsServiceTest {

    private static final int TEST_TIMEOUT_MS = 5_000;

    @Mock
    private Web3j web3jMock; // WebSocket client
    @Mock
    private Web3j web3jHttpMock; // HTTP client
    @Mock
    private LogSubscription subscriptionMock;
    @Mock
    private LogSubscription anotherSubscriptionMock;
    @Mock
    private WebSocketService webSocketServiceMock;

    private LogsService logsService;

    @Test
    @SneakyThrows
    void registerNewSubscription_historicalRequest_sendsDataAndClosesSubscription() {
        // GIVEN
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500, testExecutor, 500);

        LogsRequest historicalRequest = new LogsRequest();
        BigInteger startBlock = BigInteger.valueOf(100);
        BigInteger endBlock = BigInteger.valueOf(200);
        historicalRequest.setStartBlock(startBlock);
        historicalRequest.setEndBlock(endBlock);
        when(subscriptionMock.getClientRequest()).thenReturn(historicalRequest);

        Log fakeLog = new Log();
        fakeLog.setAddress("0x123");
        EthLog.LogObject fakeLogObject = new EthLog.LogObject();
        fakeLogObject.setAddress("0x123");
        List<EthLog.LogResult> fakeLogResults = List.of(fakeLogObject);

        EthLog ethLogMock = mock(EthLog.class);
        Request<?, EthLog> requestMock = mock(Request.class);
        when(requestMock.send()).thenReturn(ethLogMock);
        when(ethLogMock.getLogs()).thenReturn(fakeLogResults);
        doReturn(requestMock).when(web3jHttpMock).ethGetLogs(any());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);

        // THEN
        testExecutor.shutdown();
        boolean completed = testExecutor.awaitTermination(TEST_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);

        assertThat(completed)
                .withFailMessage("Test executor task did not complete in time.")
                .isTrue();

        ArgumentCaptor<List<Log>> logListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<EthFilter> ethFilterCaptor = ArgumentCaptor.forClass(EthFilter.class);

        verify(subscriptionMock).sendHistorical(logListCaptor.capture());
        verify(web3jHttpMock).ethGetLogs(ethFilterCaptor.capture());

        assertThat(logListCaptor.getValue())
                .hasSize(1)
                .extracting(Log::getAddress)
                .containsExactly("0x123");

        assertThat(ethFilterCaptor.getValue()).isNotNull();
        assertThat(ethFilterCaptor.getValue().getAddress()).isEmpty();
        assertThat(ethFilterCaptor.getValue().getFromBlock().getValue())
                .isEqualTo("0x" + startBlock.toString(16));
        assertThat(ethFilterCaptor.getValue().getToBlock().getValue())
                .isEqualTo("0x" + endBlock.toString(16));

        verify(subscriptionMock).close();
        verify(web3jHttpMock, never()).ethLogFlowable(any());
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_realtimeRequest_sendsBackfillAndDoesNotCloseSubscription() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        List<String> contractAddresses = List.of("0xspecificaddress");
        LogsRequest realtimeRequest = new LogsRequest();
        BigInteger startBlock = BigInteger.valueOf(90);
        realtimeRequest.setStartBlock(startBlock);
        realtimeRequest.setEndBlock(null);
        realtimeRequest.setContractAddresses(contractAddresses);
        when(subscriptionMock.getClientRequest()).thenReturn(realtimeRequest);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        EthLog ethLogMock = mock(EthLog.class);
        Request<?, EthLog> getLogsRequestMock = mock(Request.class);
        when(getLogsRequestMock.send()).thenReturn(ethLogMock);
        when(ethLogMock.getLogs()).thenReturn(List.of());
        doReturn(getLogsRequestMock).when(web3jHttpMock).ethGetLogs(any());

        Log fakeRealtimeLog = new Log();
        fakeRealtimeLog.setAddress("0x123");
        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.just(fakeRealtimeLog));

        final java.util.concurrent.CountDownLatch backfillLatch = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch realtimeLatch = new java.util.concurrent.CountDownLatch(1);
        doAnswer(invocation -> {
            backfillLatch.countDown();
            return null;
        }).when(subscriptionMock).sendHistorical(any());
        doAnswer(invocation -> {
            realtimeLatch.countDown();
            return null;
        }).when(subscriptionMock).sendRealtime(any());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);

        // THEN
        assertThat(backfillLatch.await(TEST_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS))
                .isTrue();
        assertThat(realtimeLatch.await(TEST_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS))
                .isTrue();

        ArgumentCaptor<List<Log>> historicalCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<Log>> realtimeCaptor = ArgumentCaptor.forClass(List.class);

        verify(subscriptionMock).sendHistorical(historicalCaptor.capture());
        verify(subscriptionMock).sendRealtime(realtimeCaptor.capture());

        assertThat(historicalCaptor.getValue()).isEmpty();
        assertThat(realtimeCaptor.getValue()).hasSize(1);
        assertThat(realtimeCaptor.getValue().getFirst().getAddress()).isEqualTo("0x123");

        ArgumentCaptor<EthFilter> historicalFilterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        ArgumentCaptor<EthFilter> realtimeFilterCaptor = ArgumentCaptor.forClass(EthFilter.class);

        verify(web3jHttpMock).ethGetLogs(historicalFilterCaptor.capture());
        verify(web3jMock).ethLogFlowable(realtimeFilterCaptor.capture());

        assertThat(historicalFilterCaptor.getValue().getFromBlock().getValue())
                .isEqualTo("0x" + startBlock.toString(16));
        assertThat(historicalFilterCaptor.getValue().getToBlock().getValue())
                .isEqualTo("0x" + latestBlock.toString(16));
        assertThat(historicalFilterCaptor.getValue().getAddress())
                .containsExactly("0xspecificaddress");

        EthFilter realtimeFilter = realtimeFilterCaptor.getValue();
        assertThat(realtimeFilter.getFromBlock().getValue())
                .isEqualTo(DefaultBlockParameterName.LATEST.getValue());
        assertThat(realtimeFilter.getToBlock().getValue())
                .isEqualTo(DefaultBlockParameterName.LATEST.getValue());
        assertThat(realtimeFilter.getAddress()).containsExactly("0xspecificaddress");

        verify(subscriptionMock, never()).close();
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithNullAddress_createsFilterForAllAddresses() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        LogsRequest specificRequest = new LogsRequest();
        specificRequest.setContractAddresses(List.of("0xspecific"));
        when(subscriptionMock.getClientRequest()).thenReturn(specificRequest);

        LogsRequest wildcardRequest = new LogsRequest();
        wildcardRequest.setContractAddresses(null);
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(wildcardRequest);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        // THEN
        ArgumentCaptor<EthFilter> filterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(web3jMock, timeout(TEST_TIMEOUT_MS).times(2)).ethLogFlowable(filterCaptor.capture());

        EthFilter finalFilter = filterCaptor.getAllValues().getLast();
        assertThat(finalFilter.getAddress()).isNull();
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithOnlySpecificAddresses_createsSpecificFilter() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        LogsRequest requestA = new LogsRequest();
        requestA.setContractAddresses(List.of("0xaaaa"));
        when(subscriptionMock.getClientRequest()).thenReturn(requestA);

        LogsRequest requestB = new LogsRequest();
        requestB.setContractAddresses(List.of("0xbbbb", "0xaaaa"));
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(requestB);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        // THEN
        ArgumentCaptor<EthFilter> filterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(web3jMock, timeout(TEST_TIMEOUT_MS).times(2)).ethLogFlowable(filterCaptor.capture());

        EthFilter finalFilter = filterCaptor.getAllValues().getLast();
        assertThat(finalFilter.getAddress())
                .containsExactlyInAnyOrder("0xaaaa", "0xbbbb");
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithNullTopics_createsFilterForAllTopics() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        LogsRequest specificRequest = new LogsRequest();
        specificRequest.setTopics(List.of("0xspecific"));
        when(subscriptionMock.getClientRequest()).thenReturn(specificRequest);

        LogsRequest wildcardRequest = new LogsRequest();
        wildcardRequest.setTopics(null);
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(wildcardRequest);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        // THEN
        ArgumentCaptor<EthFilter> filterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(web3jMock, timeout(TEST_TIMEOUT_MS).times(2)).ethLogFlowable(filterCaptor.capture());

        EthFilter finalFilter = filterCaptor.getAllValues().getLast();
        assertThat(finalFilter.getTopics()).isEmpty();
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithOnlySpecificTopics_createsSpecificFilter() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        LogsRequest requestA = new LogsRequest();
        requestA.setTopics(List.of("0xaaaa"));
        when(subscriptionMock.getClientRequest()).thenReturn(requestA);

        LogsRequest requestB = new LogsRequest();
        requestB.setTopics(List.of("0xbbbb", "0xaaaa"));
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(requestB);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        // THEN
        ArgumentCaptor<EthFilter> filterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(web3jMock, timeout(TEST_TIMEOUT_MS).times(2)).ethLogFlowable(filterCaptor.capture());

        EthFilter finalFilter = filterCaptor.getAllValues().getLast();

        List<String> listTopic = ((List<Filter.SingleTopic>) finalFilter.getTopics()
                .getFirst()
                .getValue())
                .stream()
                .map(Filter.SingleTopic::getValue)
                .collect(Collectors.toList());

        assertThat(listTopic).containsExactlyInAnyOrder("0xaaaa", "0xbbbb");
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_mixedTopics_createsAddressOnlyFilterWhenAnyTopicWildcard() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        LogsRequest withTopics = new LogsRequest();
        withTopics.setContractAddresses(List.of("0xaaa"));
        withTopics.setTopics(List.of("0xtopic1"));
        when(subscriptionMock.getClientRequest()).thenReturn(withTopics);

        LogsRequest wildcardTopics = new LogsRequest();
        wildcardTopics.setContractAddresses(List.of("0xbbb"));
        wildcardTopics.setTopics(null); // subscribeAllTopics = true
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(wildcardTopics);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        // THEN
        ArgumentCaptor<EthFilter> filterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(web3jMock, timeout(TEST_TIMEOUT_MS).times(2)).ethLogFlowable(filterCaptor.capture());

        EthFilter finalFilter = filterCaptor.getAllValues().getLast();

        assertThat(finalFilter.getAddress())
                .containsExactlyInAnyOrder("0xaaa", "0xbbb");
        assertThat(finalFilter.getTopics()).isEmpty();
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_realtimeRequest_startEqualsLatestBlock_doesNotBackfill() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500);

        LogsRequest realtimeRequest = new LogsRequest();
        BigInteger latestBlock = BigInteger.valueOf(100);
        realtimeRequest.setStartBlock(latestBlock);
        realtimeRequest.setEndBlock(null);
        when(subscriptionMock.getClientRequest()).thenReturn(realtimeRequest);

        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        Log fakeRealtimeLog = new Log();
        fakeRealtimeLog.setAddress("0xabc");
        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.just(fakeRealtimeLog));

        java.util.concurrent.CountDownLatch realtimeLatch = new java.util.concurrent.CountDownLatch(1);
        doAnswer(inv -> {
            realtimeLatch.countDown();
            return null;
        }).when(subscriptionMock).sendRealtime(any());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);

        // THEN
        assertThat(realtimeLatch.await(TEST_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS)).isTrue();
        verify(web3jHttpMock, never()).ethGetLogs(any());
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_realtimeWithWebSocket_aggregatesAddressesAndTopics() {
        // GIVEN
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;

        logsService = new LogsService(
                web3jMock,
                web3jHttpFactory,
                500,
                webSocketServiceMock,
                testExecutor,
                0
        );

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jHttpMock).ethBlockNumber();

        EthLog ethLogMock = mock(EthLog.class);
        Request<?, EthLog> getLogsRequestMock = mock(Request.class);
        when(getLogsRequestMock.send()).thenReturn(ethLogMock);
        when(ethLogMock.getLogs()).thenReturn(List.of());
        doReturn(getLogsRequestMock).when(web3jHttpMock).ethGetLogs(any());

        LogsRequest request1 = new LogsRequest();
        request1.setStartBlock(BigInteger.valueOf(90));
        request1.setContractAddresses(List.of("0xaaa"));
        request1.setTopics(List.of("0xtopic1"));
        when(subscriptionMock.getClientRequest()).thenReturn(request1);

        LogsRequest request2 = new LogsRequest();
        request2.setStartBlock(BigInteger.valueOf(95));
        request2.setContractAddresses(List.of("0xbbb"));
        request2.setTopics(List.of("0xtopic2"));
        when(anotherSubscriptionMock.getClientRequest()).thenReturn(request2);

        ArgumentCaptor<Request> wsRequestCaptor = ArgumentCaptor.forClass(Request.class);
        doReturn(Flowable.empty())
                .when(webSocketServiceMock)
                .subscribe(wsRequestCaptor.capture(), anyString(), any());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);
        logsService.registerNewSubscription(anotherSubscriptionMock);

        testExecutor.shutdown();
        testExecutor.awaitTermination(TEST_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);

        // THEN
        List<Request> allWsRequests = wsRequestCaptor.getAllValues();
        assertThat(allWsRequests).hasSize(2);

        Request<?, ?> wsRequest = allWsRequests.getLast();
        assertThat(wsRequest.getMethod()).isEqualTo("eth_subscribe");

        List<?> params = wsRequest.getParams();
        assertThat(params).hasSize(2);
        assertThat(params.get(0)).isEqualTo("logs");

        Object filterParamsObj = params.get(1);
        assertThat(filterParamsObj).isInstanceOf(Map.class);
        Map<String, Object> filterParams = (Map<String, Object>) filterParamsObj;

        assertThat(filterParams.get("address"))
                .asList()
                .containsExactlyInAnyOrder("0xaaa", "0xbbb");

        List<?> topicsOuter = (List<?>) filterParams.get("topics");
        assertThat(topicsOuter).hasSize(1);
        List<String> slot0Topics = (List<String>) topicsOuter.get(0);
        assertThat(slot0Topics).containsExactlyInAnyOrder("0xtopic1", "0xtopic2");
    }

    @Test
    @SneakyThrows
    void pushHistoricalData_whenTooManyResultsError_recursesAndSkipsWithoutSending() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 10, null,
                Executors.newSingleThreadExecutor(), 0);

        EthLog ethLogError = mock(EthLog.class);
        when(ethLogError.hasError()).thenReturn(true);
        Response.Error error = new Response.Error(1, "query returned more than 10000 results");
        when(ethLogError.getError()).thenReturn(error);

        Request<?, EthLog> getLogsRequestMock = mock(Request.class);
        when(getLogsRequestMock.send()).thenReturn(ethLogError);
        doReturn(getLogsRequestMock).when(web3jHttpMock).ethGetLogs(any());

        LogsRequest request = new LogsRequest();
        when(subscriptionMock.getClientRequest()).thenReturn(request);

        // WHEN
        invokePushHistoricalData(logsService, subscriptionMock,
                BigInteger.ONE, BigInteger.valueOf(3));

        // THEN
        verify(web3jHttpMock, atLeastOnce()).ethGetLogs(any());
        verify(subscriptionMock, never()).sendHistorical(any());
    }

    @Test
    @SneakyThrows
    void pushHistoricalData_onClientConnectionException_recreatesHttpClientAndRetries() {
        // GIVEN
        Web3j httpMock1 = mock(Web3j.class);
        Web3j httpMock2 = mock(Web3j.class);

        final int[] counter = {0};
        Supplier<Web3j> factory = () -> counter[0]++ == 0 ? httpMock1 : httpMock2;

        logsService = new LogsService(web3jMock, factory, 500, null,
                Executors.newSingleThreadExecutor(), 0);

        Request<?, EthLog> failingRequest = mock(Request.class);
        when(failingRequest.send()).thenThrow(new ClientConnectionException("boom"));
        doReturn(failingRequest).when(httpMock1).ethGetLogs(any());

        EthLog okEthLog = mock(EthLog.class);
        EthLog.LogObject fakeLogObject = new EthLog.LogObject();
        fakeLogObject.setAddress("0x1");
        when(okEthLog.getLogs()).thenReturn(List.of(fakeLogObject));

        Request<?, EthLog> okRequest = mock(Request.class);
        when(okRequest.send()).thenReturn(okEthLog);
        doReturn(okRequest).when(httpMock2).ethGetLogs(any());

        LogsRequest logsRequest = new LogsRequest();
        when(subscriptionMock.getClientRequest()).thenReturn(logsRequest);

        // WHEN
        invokePushHistoricalData(logsService, subscriptionMock,
                BigInteger.ONE, BigInteger.ONE);

        // THEN
        verify(httpMock1).ethGetLogs(any());
        verify(httpMock2).ethGetLogs(any());

        ArgumentCaptor<List<Log>> logsCaptor = ArgumentCaptor.forClass(List.class);
        verify(subscriptionMock).sendHistorical(logsCaptor.capture());
        assertThat(logsCaptor.getValue()).hasSize(1);
        assertThat(logsCaptor.getValue().getFirst().getAddress()).isEqualTo("0x1");
    }

    @Test
    @SneakyThrows
    void pushHistoricalData_whenUnknownError_throwsRuntimeException() {
        // GIVEN
        Supplier<Web3j> web3jHttpFactory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, web3jHttpFactory, 500, null,
                Executors.newSingleThreadExecutor(), 0);

        EthLog ethLogError = mock(EthLog.class);
        when(ethLogError.hasError()).thenReturn(true);
        Response.Error error = new Response.Error(1, "some other error");
        when(ethLogError.getError()).thenReturn(error);

        Request<?, EthLog> getLogsRequestMock = mock(Request.class);
        when(getLogsRequestMock.send()).thenReturn(ethLogError);
        doReturn(getLogsRequestMock).when(web3jHttpMock).ethGetLogs(any());

        LogsRequest request = new LogsRequest();
        when(subscriptionMock.getClientRequest()).thenReturn(request);

        // WHEN / THEN
        assertThatThrownBy(() ->
                invokePushHistoricalData(logsService, subscriptionMock,
                        BigInteger.ONE, BigInteger.ONE)
        )
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to fetch historical logs");
    }

    // ---------- НОВЫЕ ТЕСТЫ ДЛЯ processNewSubscription И handleSubscriptionRemoval ----------

    @Test
    @SneakyThrows
    void processNewSubscription_clientConnectionExceptionWhenFetchingLatestBlock_recreatesHttpClientAndRetries() {
        // GIVEN
        Web3j httpMock1 = mock(Web3j.class);
        Web3j httpMock2 = mock(Web3j.class);
        final int[] counter = {0};
        Supplier<Web3j> factory = () -> counter[0]++ == 0 ? httpMock1 : httpMock2;

        logsService = new LogsService(web3jMock, factory, 500, null,
                Executors.newSingleThreadExecutor(), 0);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // первый HTTP-client: кидает ClientConnectionException при запросе блока
        Request<?, EthBlockNumber> failingReq = mock(Request.class);
        when(failingReq.send()).thenThrow(new ClientConnectionException("boom"));
        doReturn(failingReq).when(httpMock1).ethBlockNumber();

        // второй HTTP-client: возвращает корректный номер блока
        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber okBlockNumber = mock(EthBlockNumber.class);
        when(okBlockNumber.getBlockNumber()).thenReturn(latestBlock);

        Request<?, EthBlockNumber> okReq = mock(Request.class);
        when(okReq.send()).thenReturn(okBlockNumber);
        doReturn(okReq).when(httpMock2).ethBlockNumber();

        LogsRequest request = new LogsRequest();
        request.setStartBlock(latestBlock); // start == end → backfill не запускается
        request.setEndBlock(null);          // awaitingForRealTimeData = true
        when(subscriptionMock.getClientRequest()).thenReturn(request);

        // WHEN (никакого исключения наружу быть не должно)
        invokeProcessNewSubscription(logsService, subscriptionMock);

        // THEN
        verify(httpMock1).ethBlockNumber();
        verify(httpMock2).ethBlockNumber();
        verify(subscriptionMock, never()).error(any());
    }

    @Test
    @SneakyThrows
    void processNewSubscription_unexpectedException_callsErrorAndRemovesSubscription() {
        // GIVEN
        Supplier<Web3j> factory = () -> web3jHttpMock;
        logsService = new LogsService(
                web3jMock,
                factory,
                500,
                null,
                Executors.newSingleThreadExecutor(),
                0
        );

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        LogsRequest request = new LogsRequest();
        request.setStartBlock(BigInteger.ONE);
        request.setEndBlock(null); // awaitingForRealTimeData = true, subscription goes to the list
        when(subscriptionMock.getClientRequest()).thenReturn(request);

        // ethBlockNumber().send() throws a non-ClientConnectionException
        Request blockReq = mock(Request.class);
        doReturn(blockReq).when(web3jHttpMock).ethBlockNumber();
        when(blockReq.send()).thenThrow(new IllegalStateException("boom"));

        // WHEN
        invokeProcessNewSubscription(logsService, subscriptionMock);

        // THEN: outer catch (Exception) is triggered
        verify(web3jHttpMock).ethBlockNumber();
        verify(subscriptionMock).error(any());
        verify(subscriptionMock).close(); // called from handleSubscriptionRemoval

        // subscription is removed from internal list
        Field subsField = LogsService.class.getDeclaredField("subscriptions");
        subsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<LogSubscription> subs = (List<LogSubscription>) subsField.get(logsService);
        assertThat(subs).doesNotContain(subscriptionMock);
    }

    @Test
    @SneakyThrows
    void handleSubscriptionRemoval_whenCloseThrows_wrapsExceptionAndRebuildsAggregatedSubscription() {
        // GIVEN
        Supplier<Web3j> factory = () -> web3jHttpMock;
        logsService = new LogsService(web3jMock, factory, 500);

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.empty());

        // prepare subscription with non-null client request to avoid NPE in buildRealtimeFilter
        LogsRequest clientRequest = new LogsRequest();
        clientRequest.setStartBlock(BigInteger.ONE);
        clientRequest.setEndBlock(null);
        when(subscriptionMock.getClientRequest()).thenReturn(clientRequest);

        Field subsField = LogsService.class.getDeclaredField("subscriptions");
        subsField.setAccessible(true);
        List<LogSubscription> subs = (List<LogSubscription>) subsField.get(logsService);
        subs.add(subscriptionMock);

        Method rebuildMethod = LogsService.class.getDeclaredMethod("rebuildAggregatedWeb3jSubscription");
        rebuildMethod.setAccessible(true);
        rebuildMethod.invoke(logsService); // creates non-null aggregatedSubscription

        doAnswer(invocation -> {
            throw new IllegalStateException("close failed");
        }).when(subscriptionMock).close();

        // WHEN / THEN
        assertThatThrownBy(() ->
                invokeHandleSubscriptionRemoval(logsService, subscriptionMock)
        )
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("close failed");

        List<LogSubscription> subsAfter = (List<LogSubscription>) subsField.get(logsService);
        assertThat(subsAfter).doesNotContain(subscriptionMock);

        Field aggField = LogsService.class.getDeclaredField("aggregatedSubscription");
        aggField.setAccessible(true);
        Object agg = aggField.get(logsService);
        assertThat(agg).isNull();
    }


    // ---------- reflection helpers ----------

    private static void invokePushHistoricalData(
            LogsService service,
            LogSubscription subscription,
            BigInteger startBlock,
            BigInteger endBlock
    ) throws Exception {
        Method method = LogsService.class.getDeclaredMethod(
                "pushHistoricalData",
                LogSubscription.class,
                BigInteger.class,
                BigInteger.class
        );
        method.setAccessible(true);
        try {
            method.invoke(service, subscription, startBlock, endBlock);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw e;
        }
    }

    private static void invokeProcessNewSubscription(
            LogsService service,
            LogSubscription subscription
    ) throws Exception {
        Method method = LogsService.class.getDeclaredMethod(
                "processNewSubscription",
                LogSubscription.class
        );
        method.setAccessible(true);
        method.invoke(service, subscription);
    }

    private static void invokeHandleSubscriptionRemoval(
            LogsService service,
            LogSubscription subscription
    ) throws Exception {
        Method method = LogsService.class.getDeclaredMethod(
                "handleSubscriptionRemoval",
                LogSubscription.class
        );
        method.setAccessible(true);
        try {
            method.invoke(service, subscription);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw e;
        }
    }
}
