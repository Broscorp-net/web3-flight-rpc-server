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
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.request.Filter;
import org.web3j.protocol.core.methods.response.EthBlockNumber;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
    private Web3j web3jMock;

    @Mock
    private LogSubscription subscriptionMock;
    @Mock
    private LogSubscription anotherSubscriptionMock;

    private LogsService logsService;

    @Test
    @SneakyThrows
    void registerNewSubscription_historicalRequest_sendsDataAndClosesSubscription() {
        // GIVEN
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        logsService = new LogsService(web3jMock, 500, testExecutor);

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
        doReturn(requestMock).when(web3jMock).ethGetLogs(any());

        // WHEN
        logsService.registerNewSubscription(subscriptionMock);

        // THEN
        testExecutor.shutdown();
        boolean completed = testExecutor.awaitTermination(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(completed)
                .withFailMessage("Test executor task did not complete in time.")
                .isTrue();

        ArgumentCaptor<List<Log>> logListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<EthFilter> ethFilterCaptor = ArgumentCaptor.forClass(EthFilter.class);
        verify(subscriptionMock).sendHistorical(logListCaptor.capture());
        verify(web3jMock).ethGetLogs(ethFilterCaptor.capture());

        assertThat(logListCaptor.getValue())
                .hasSize(1)
                .extracting(Log::getAddress)
                .containsExactly("0x123");
        assertThat(ethFilterCaptor.getValue()).isNotNull();
        assertThat(ethFilterCaptor.getValue().getAddress()).isEmpty();
        assertThat(ethFilterCaptor.getValue().getFromBlock().getValue()).isEqualTo("0x" + startBlock.toString(16));
        assertThat(ethFilterCaptor.getValue().getToBlock().getValue()).isEqualTo("0x" + endBlock.toString(16));

        verify(subscriptionMock).close();
        verify(web3jMock, never()).ethLogFlowable(any());
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_realtimeRequest_sendsBackfillAndDoesNotCloseSubscription() {
        // GIVEN
        logsService = new LogsService(web3jMock, 500);

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
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

        EthLog ethLogMock = mock(EthLog.class);
        Request<?, EthLog> getLogsRequestMock = mock(Request.class);
        when(getLogsRequestMock.send()).thenReturn(ethLogMock);
        when(ethLogMock.getLogs()).thenReturn(List.of());
        doReturn(getLogsRequestMock).when(web3jMock).ethGetLogs(any());

        Log fakeRealtimeLog = new Log();
        fakeRealtimeLog.setAddress("0x123");

        when(web3jMock.ethLogFlowable(any())).thenReturn(Flowable.just(fakeRealtimeLog));

        final CountDownLatch backfillLatch = new CountDownLatch(1);
        final CountDownLatch realtimeLatch = new CountDownLatch(1);

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
        assertThat(backfillLatch.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .isTrue();
        assertThat(realtimeLatch.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
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

        verify(web3jMock).ethGetLogs(historicalFilterCaptor.capture());
        verify(web3jMock).ethLogFlowable(realtimeFilterCaptor.capture());

        assertThat(historicalFilterCaptor.getValue().getFromBlock().getValue()).isEqualTo("0x" + startBlock.toString(16));
        assertThat(historicalFilterCaptor.getValue().getToBlock().getValue()).isEqualTo("0x" + latestBlock.toString(16));
        assertThat(historicalFilterCaptor.getValue().getAddress()).containsExactly("0xspecificaddress");

        EthFilter realtimeFilter = realtimeFilterCaptor.getValue();
        assertThat(realtimeFilter.getFromBlock().getValue()).isEqualTo(DefaultBlockParameterName.LATEST.getValue());
        assertThat(realtimeFilter.getToBlock().getValue()).isEqualTo(DefaultBlockParameterName.LATEST.getValue());
        assertThat(realtimeFilter.getAddress()).containsExactly("0xspecificaddress");

        verify(subscriptionMock, never()).close();
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithNullAddress_createsFilterForAllAddresses() {
        // GIVEN
        logsService = new LogsService(web3jMock, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

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
        logsService = new LogsService(web3jMock, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

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
        assertThat(finalFilter.getAddress()).containsExactlyInAnyOrder("0xaaaa", "0xbbbb");
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_requestWithNullTopics_createsFilterForAllTopics() {
        // GIVEN
        logsService = new LogsService(web3jMock, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

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
        logsService = new LogsService(web3jMock, 500);

        BigInteger latestBlock = BigInteger.valueOf(100);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

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
}