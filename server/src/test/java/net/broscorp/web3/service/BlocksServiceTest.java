package net.broscorp.web3.service;

import io.reactivex.Flowable;
import lombok.SneakyThrows;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.subscription.BlockSubscription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlockNumber;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BlocksServiceTest {
    private static final int TEST_TIMEOUT_MS = 5_000;

    @Mock
    private Web3j web3jMock;

    @Mock
    private BlockSubscription subscriptionMock;

    private BlocksService blocksService;

    @Test
    @SneakyThrows
    void registerNewSubscription_historicalRequest_sendsDataAndClosesSubscription() {
        // GIVEN
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();
        blocksService = new BlocksService(web3jMock, 500, testExecutor);

        BlocksRequest historicalRequest = new BlocksRequest();
        BigInteger startBlock = BigInteger.valueOf(100);
        BigInteger endBlock = BigInteger.valueOf(199);
        historicalRequest.setStartBlock(startBlock);
        historicalRequest.setEndBlock(endBlock);
        when(subscriptionMock.getClientRequest()).thenReturn(historicalRequest);

        Request<?, EthBlock> ethGetBlockByNumberMock = mock(Request.class);
        EthBlock.Block fakeBlock = new EthBlock.Block();
        fakeBlock.setNumber("0x123");
        EthBlock fakeEthBlockWrapper = new EthBlock();
        fakeEthBlockWrapper.setResult(fakeBlock);
        when(ethGetBlockByNumberMock.send()).thenReturn(fakeEthBlockWrapper);

        doReturn(ethGetBlockByNumberMock).when(web3jMock).ethGetBlockByNumber(any(), eq(false));

        // WHEN
        blocksService.registerNewSubscription(subscriptionMock);

        // THEN
        testExecutor.shutdown();
        boolean completed = testExecutor.awaitTermination(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(completed)
                .withFailMessage("Test executor task did not complete in time.")
                .isTrue();

        ArgumentCaptor<List<EthBlock.Block>> blockListCaptor = ArgumentCaptor.forClass(List.class);
        verify(subscriptionMock).sendHistorical(blockListCaptor.capture());

        assertThat(blockListCaptor.getValue())
                .hasSize(100);
        assertThat(blockListCaptor.getValue().stream().allMatch(block -> block.getNumberRaw().equals("0x123")))
                .isTrue();

        verify(subscriptionMock).close();
        verify(web3jMock, never()).ethLogFlowable(any());
    }

    @Test
    @SneakyThrows
    void registerNewSubscription_realtimeRequest_sendsBackfillAndDoesNotCloseSubscription() {
        // GIVEN
        blocksService = new BlocksService(web3jMock, 500);

        BlocksRequest historicalRequest = new BlocksRequest();
        BigInteger startBlock = BigInteger.valueOf(100);
        historicalRequest.setStartBlock(startBlock);
        when(subscriptionMock.getClientRequest()).thenReturn(historicalRequest);

        BigInteger latestBlock = BigInteger.valueOf(199);
        EthBlockNumber ethBlockNumberMock = mock(EthBlockNumber.class);
        Request<?, EthBlockNumber> blockNumRequestMock = mock(Request.class);
        when(blockNumRequestMock.send()).thenReturn(ethBlockNumberMock);
        when(ethBlockNumberMock.getBlockNumber()).thenReturn(latestBlock);
        doReturn(blockNumRequestMock).when(web3jMock).ethBlockNumber();

        Request<?, EthBlock> ethGetBlockByNumberMock = mock(Request.class);
        EthBlock.Block fakeHistoricalBlock = new EthBlock.Block();
        fakeHistoricalBlock.setNumber("0x123");
        EthBlock fakeEthBlockWrapper = new EthBlock();
        fakeEthBlockWrapper.setResult(fakeHistoricalBlock);
        when(ethGetBlockByNumberMock.send()).thenReturn(fakeEthBlockWrapper);
        EthBlock.Block fakeRealtimeBlock = new EthBlock.Block();
        fakeRealtimeBlock.setNumber("0x124");
        EthBlock fakeEthRealtimeBlockWrapper = new EthBlock();
        fakeEthRealtimeBlockWrapper.setResult(fakeRealtimeBlock);

        doReturn(ethGetBlockByNumberMock).when(web3jMock).ethGetBlockByNumber(any(), eq(false));

        when(web3jMock.blockFlowable(false)).thenReturn(Flowable.just(fakeEthRealtimeBlockWrapper));

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
        blocksService.registerNewSubscription(subscriptionMock);

        // THEN
        assertThat(backfillLatch.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .isTrue();
        assertThat(realtimeLatch.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .isTrue();

        ArgumentCaptor<List<EthBlock.Block>> historicalBlockCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<EthBlock.Block>> realtimeBlockCaptor = ArgumentCaptor.forClass(List.class);
        verify(subscriptionMock).sendHistorical(historicalBlockCaptor.capture());
        verify(subscriptionMock).sendRealtime(realtimeBlockCaptor.capture());

        assertThat(historicalBlockCaptor.getValue())
                .hasSize(100);
        assertThat(historicalBlockCaptor.getValue().stream().allMatch(block -> block.getNumberRaw().equals("0x123")))
                .isTrue();
        assertThat(realtimeBlockCaptor.getValue())
                .hasSize(1);
        assertThat(realtimeBlockCaptor.getValue().stream().allMatch(block -> block.getNumberRaw().equals("0x124")))
                .isTrue();

        verify(subscriptionMock, never()).close();
        verify(web3jMock, times(1)).blockFlowable(false);
    }
}