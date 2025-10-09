package net.broscorp.web3.subscription;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.ClientRequest;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-safe (except for {@code setOnCancelHandler} and {@code setOnErrorHandler}, which should only be called
 * by the thread setting up this object) component that manages the full lifecycle
 * of a data stream to a single Flight client.
 * <p>
 * The state of the stream, realtime (streaming) or sending historical data (backfilling)
 * is determined by the request associated with the {@code Subscription} instance.
 * <p>
 *     The state of the stream can be idempotently transitioned to realtime by calling {@code completeBackfill} method.
 * @param <T> The type of the source data item (e.g., {@code Log}).
 * @param <R> The type of the client request DTO.
 */
@Slf4j
@ToString(of = {"clientRequest", "currentState"})
public abstract class Subscription<T, R extends ClientRequest> implements AutoCloseable {

    /**
     * Enum representing the state that the data stream in a Subscription is in.
     */
    private enum State {
        /**
         * The subscription is processing a historical backfill.
         * New real-time items are buffered.
         */
        BACKFILLING,
        /**
         * The subscription is processing real-time data.
         * New items are sent immediately.
         */
        STREAMING
    }

    private final FlightProducer.ServerStreamListener listener;
    private final VectorSchemaRoot root;
    private final BufferAllocator allocator;
    @Getter
    protected final R clientRequest;
    private final ExecutorService executor;

    private final AtomicReference<CompletableFuture<Void>> lastSentFuture =
            new AtomicReference<>(CompletableFuture.completedFuture(null));
    // Object used to synchronize logic changing the state of the stream from BACKFILLING to STREAMING.
    private final Object stateLock = new Object();
    private final AtomicReference<State> currentState;
    // Accumulates real-time data received while the historical request was being fulfilled.
    private final ConcurrentLinkedQueue<T> realtimeBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);
    private Runnable errorHandler;

    protected Subscription(FlightProducer.ServerStreamListener listener,
                           VectorSchemaRoot root,
                           BufferAllocator allocator,
                           R clientRequest,
                           ExecutorService executor) {
        this.listener = listener;
        this.root = root;
        this.allocator = allocator;
        this.clientRequest = clientRequest;
        this.executor = executor;

        if (clientRequest.needsHistoricalData()) {
            this.currentState = new AtomicReference<>(State.BACKFILLING);
        } else {
            this.currentState = new AtomicReference<>(State.STREAMING);
        }

        listener.start(root);
    }

    /**
     * The entry point for all new realtime data, which gets sent to this Flight client.
     * <p>
     * If the subscription is currently sending historical data (backfilling), realtime data gets buffered to
     * get sent later.
     * @param data A list of new data items.
     */
    public void sendRealtime(List<T> data) {
        if (currentState.get() == State.STREAMING) {
            enqueueSendOperation(data);
            return;
        }

        synchronized (stateLock) {
            if (currentState.get() == State.BACKFILLING) {
                realtimeBuffer.addAll(data);
            } else {
                enqueueSendOperation(data);
            }
        }
    }

    /**
     * The entry point for all new historical data, which gets sent to this Flight client.
     *
     * @param data A list of new data items.
     * @throws RuntimeException if Subscription is already in realtime mode.
     */
    public void sendHistorical(List<T> data) {
        if (currentState.get() == State.BACKFILLING) {
            enqueueSendOperation(data);
        } else {
            throw new RuntimeException("Subscription cannot, or cannot anymore, send historical data.");
        }
    }

    /**
     * Transitions the stream to realtime (streaming) and enqueues any buffered data for sending.
     * <p>
     * This method must be called on what is a Subscription that should transition to sending realtime data, at the point
     * when it should start doing so.
     */
    public void completeBackfill() {
        log.info("Transitioning subscription to realtime for: {}", clientRequest);
        if (currentState.get() == State.BACKFILLING) {
            List<T> bufferedItems;
            synchronized (stateLock) {
                bufferedItems = new ArrayList<>(realtimeBuffer);
                realtimeBuffer.clear();

                if (!bufferedItems.isEmpty()) {
                    log.info("Draining {} buffered real-time events for subscription: {}",
                            bufferedItems.size(), clientRequest);
                    this.enqueueSendOperation(bufferedItems);
                }

                currentState.set(State.STREAMING);
                log.info("Subscription state switched to STREAMING for: {}", clientRequest);
            }
        }
    }

    private void enqueueSendOperation(List<T> data) {
        lastSentFuture.updateAndGet(currentFuture ->
                currentFuture.thenRunAsync(() -> executeSend(data), executor));
    }

    private void executeSend(List<T> data) {
        if (data == null || data.isEmpty() || listener.isCancelled() || isTerminated.get()) {
            return;
        }
        try {
            List<T> filteredData = filter(data);
            if (filteredData.isEmpty()) return;

            populate(filteredData, root);
            if (root.getRowCount() > 0) {
                listener.putNext();
            }
        } catch (IllegalStateException e) {
            log.warn("Skipped putNext(): stream already terminated or cancelled");
            if (!isTerminated.get()) {
                this.error(e);
            }
        } catch (Exception e) {
            log.error("Error sending data. Terminating stream for: {}", clientRequest, e);
            this.error(e);
        } finally {
            root.clear();
        }
    }

    protected abstract List<T> filter(List<T> originalData);

    protected abstract void populate(List<T> data, VectorSchemaRoot root);

    @Override
    public void close() throws Exception {
        log.debug("Closing subscription and releasing resources for request: {}", clientRequest);
        if (isTerminated.compareAndSet(false, true)) {
            lastSentFuture.get().whenComplete((res, err) -> listener.completed());
        }
        lastSentFuture.get().join();
        AutoCloseables.close(root, allocator);
    }

    public void error(Exception e) {
        if (isTerminated.compareAndSet(false, true)) {
            listener.error(e);
            if (errorHandler != null) {
                errorHandler.run();
            }
        }
    }

    /**
     * Sets a callback to run when the client cancels a subscription.
     */
    public void setOnCancelHandler(Runnable onCancel) {
        listener.setOnCancelHandler(onCancel);
    }

    /**
     * Sets a callback to run when there is an error during sending of the data.
     */
    public void setOnErrorHandler(Runnable errorHandler) {
        this.errorHandler = errorHandler;
    }
}