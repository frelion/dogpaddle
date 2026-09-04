package dev.dogpaddle.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.kafka.connect.source.SourceRecord;

/** Owns one Debezium Engine and its single-delivery ACK protocol. */
final class ConnectorRuntime {
    private enum State {
        CREATED,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        FAILED,
        DISPOSED
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final DeliveryCodec codec = new DeliveryCodec();
    private final DeliveryExchange exchange = new DeliveryExchange();
    private final LongSupplier nextToken;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final ConnectorConfiguration configuration;
    private final OffsetStoreRegistry.Entry offsets;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private final ShutdownWorker shutdownWorker;
    private final int maximumDeliveryBytes;

    private volatile Thread engineThread;
    private volatile String completionMessage;
    private volatile Throwable failure;
    private volatile long lastAckToken;
    private boolean abandonStarted;

    private ConnectorRuntime(
            ConnectorConfiguration configuration,
            Checkpoint checkpoint,
            int maximumDeliveryBytes,
            LongSupplier nextToken) {
        if (maximumDeliveryBytes < DeliveryCodec.MINIMUM_MAXIMUM_BYTES) {
            throw new IllegalArgumentException(
                    "maximum delivery bytes must be at least "
                            + DeliveryCodec.MINIMUM_MAXIMUM_BYTES);
        }
        this.configuration = configuration;
        this.maximumDeliveryBytes = maximumDeliveryBytes;
        this.nextToken = nextToken;
        try {
            this.offsets = OffsetStoreRegistry.register(
                    configuration.engineName(), configuration.connectorClass(), checkpoint);
        }
        catch (Throwable error) {
            codec.close();
            throw error;
        }

        try {
            DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> consumer =
                    new DebeziumEngine.ChangeConsumer<>() {
                        @Override
                        public void handleBatch(
                                List<RecordChangeEvent<SourceRecord>> records,
                                DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
                                throws InterruptedException {
                            ConnectorRuntime.this.handleBatch(records, committer);
                        }

                        @Override
                        public boolean supportsTombstoneEvents() {
                            return true;
                        }
                    };
            DebeziumEngine.Builder<RecordChangeEvent<SourceRecord>> builder =
                    DebeziumEngine.create(ChangeEventFormat.of(Connect.class));
            builder.using(configuration.properties());
            builder.notifying(consumer);
            builder.using(this::completed);
            builder.using(new DebeziumEngine.ConnectorCallback() {
                @Override
                public void pollingStarted() {
                    state.compareAndSet(State.STARTING, State.RUNNING);
                }

                @Override
                public void pollingStopped() {
                    state.compareAndSet(State.RUNNING, State.STOPPING);
                }
            });
            builder.using(OffsetCommitPolicy.always());
            engine = builder.build();
        }
        catch (Throwable error) {
            codec.close();
            OffsetStoreRegistry.unregister(configuration.engineName(), offsets);
            throw error;
        }
        shutdownWorker = new ShutdownWorker(
                "dogpaddle-debezium-shutdown-" + configuration.engineName(),
                this::closeAndJoinEngine);
    }

    static ConnectorRuntime create(
            byte[] configurationJson,
            byte[] checkpointBytes,
            int maximumDeliveryBytes,
            LongSupplier nextToken) {
        if (maximumDeliveryBytes < DeliveryCodec.MINIMUM_MAXIMUM_BYTES) {
            throw new IllegalArgumentException(
                    "maximum delivery bytes must be at least "
                            + DeliveryCodec.MINIMUM_MAXIMUM_BYTES);
        }
        ObjectMapper mapper = new ObjectMapper();
        ConnectorConfiguration configuration =
                ConnectorConfiguration.parse(configurationJson, mapper);
        Checkpoint checkpoint = checkpointBytes == null
                ? new Checkpoint(
                        configuration.engineName(), configuration.connectorClass(), java.util.Map.of())
                : CheckpointCodec.decode(checkpointBytes);
        checkpoint.requireBinding(
                configuration.engineName(), configuration.connectorClass());
        int checkpointLength = checkpointBytes == null
                ? CheckpointCodec.encode(checkpoint).length
                : checkpointBytes.length;
        int connectorMinimum = Math.addExact(
                DeliveryCodec.MINIMUM_BYTES_EXCLUDING_CHECKPOINT, checkpointLength);
        if (maximumDeliveryBytes < connectorMinimum) {
            throw new IllegalArgumentException(
                    "maximum delivery bytes must be at least " + connectorMinimum
                            + " for the initial checkpoint");
        }
        return new ConnectorRuntime(
                configuration, checkpoint, maximumDeliveryBytes, nextToken);
    }

    synchronized void start() {
        if (!state.compareAndSet(State.CREATED, State.STARTING)) {
            throw new IllegalStateException(
                    "connector can only start once; current state is " + stateName());
        }
        Thread thread = new Thread(
                this::runEngine,
                "dogpaddle-debezium-engine-" + configuration.engineName());
        thread.setDaemon(false);
        engineThread = thread;
        thread.start();
    }

    byte[] poll(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("poll timeout must be non-negative");
        }
        requireNotDisposed();
        requireRunningForPoll();
        try {
            DeliveryExchange.Delivery delivery = exchange.poll(timeoutMillis);
            State observed = state.get();
            if (observed == State.FAILED) {
                throw new IllegalStateException("Debezium connector failed", failure);
            }
            if (observed == State.RUNNING) {
                return delivery == null ? null : delivery.encoded();
            }
            throw new IllegalStateException(
                    "Debezium connector stopped while polling; current state is " + stateName());
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("poll interrupted", error);
        }
    }

    boolean ack(long token, long timeoutMillis) {
        requireNotDisposed();
        boolean settled = exchange.ack(token, timeoutMillis);
        if (settled) {
            lastAckToken = token;
        }
        return settled;
    }

    boolean stop(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("stop timeout must be non-negative");
        }
        long startedAt = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (this) {
            State observed = state.get();
            if (observed == State.DISPOSED) {
                throw new IllegalStateException("connector is disposed");
            }
            if (observed == State.STOPPED) {
                Thread thread = engineThread;
                if (!shutdownWorker.hasStarted()
                        && (thread == null || !thread.isAlive())) {
                    return true;
                }
                exchange.close();
            }
            else if (observed == State.CREATED) {
                exchange.close();
                state.set(State.STOPPED);
                return true;
            }
            else {
                state.updateAndGet(current -> current == State.FAILED
                        ? current
                        : State.STOPPING);
                exchange.close();
            }
        }
        return shutdownWorker.awaitUntil(startedAt, timeoutNanos);
    }

    synchronized void dispose() {
        State observed = state.get();
        if (observed == State.DISPOSED) {
            throw new IllegalStateException("connector is already disposed");
        }
        Thread thread = engineThread;
        if (observed != State.STOPPED && observed != State.FAILED) {
            throw new IllegalStateException(
                    "connector must be stopped before dispose; current state is " + stateName());
        }
        if (thread != null && thread.isAlive()) {
            throw new IllegalStateException("connector thread is still running");
        }
        OffsetStoreRegistry.unregister(configuration.engineName(), offsets);
        codec.close();
        state.set(State.DISPOSED);
    }

    void abandon(Runnable reclaim) {
        synchronized (this) {
            if (state.get() == State.DISPOSED || abandonStarted) {
                return;
            }
            abandonStarted = true;
            // Abort the outstanding delivery before the asynchronous worker is
            // scheduled. This path never signals an ACK.
            exchange.close();
        }
        Thread cleanup = new Thread(
                () -> abandonInBackground(reclaim),
                "dogpaddle-debezium-reclaim-" + configuration.engineName());
        cleanup.setDaemon(true);
        cleanup.start();
    }

    synchronized boolean isDisposed() {
        return state.get() == State.DISPOSED;
    }

    byte[] status() {
        DeliveryExchange.Snapshot exchangeSnapshot = exchange.snapshot();
        ObjectNode root = mapper.createObjectNode();
        root.put("protocol", 1);
        root.put("kind", "status");
        root.put("state", stateName());
        root.put("outstanding", exchangeSnapshot.hasOutstanding());
        root.put("record_count", exchangeSnapshot.recordCount());
        if (exchangeSnapshot.token() == null) {
            root.putNull("token");
        }
        else {
            root.put("token", exchangeSnapshot.token());
        }
        root.put("last_ack_token", lastAckToken);
        Thread thread = engineThread;
        root.put("engine_thread_alive", thread != null && thread.isAlive());
        if (completionMessage == null) {
            root.putNull("message");
        }
        else {
            root.put("message", completionMessage);
        }
        if (failure == null) {
            root.putNull("error");
        }
        else {
            root.put("error", describe(failure));
        }
        String failureKind = DeliveryCodec.failureKind(failure);
        if (failureKind == null) {
            root.putNull("failure_kind");
        }
        else {
            root.put("failure_kind", failureKind);
        }
        try {
            return mapper.writeValueAsBytes(root);
        }
        catch (JsonProcessingException error) {
            throw new IllegalStateException("cannot encode runtime status", error);
        }
    }

    private void handleBatch(
            List<RecordChangeEvent<SourceRecord>> events,
            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
            throws InterruptedException {
        if (events.isEmpty()) {
            committer.markBatchFinished();
            return;
        }
        List<SourceRecord> records = events.stream()
                .map(RecordChangeEvent::record)
                .toList();
        if (records.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalStateException("Debezium emitted a null SourceRecord");
        }

        OffsetStoreRegistry.PreparedCheckpoint prepared = offsets.preview(records);
        long token = nextToken.getAsLong();
        byte[] encoded = codec.encode(
                token, prepared.encoded(), records, maximumDeliveryBytes);
        DeliveryExchange.Delivery delivery = exchange.install(
                token, records, prepared, encoded);

        Throwable deliveryFailure = null;
        try {
            if (exchange.awaitDecision(delivery) == DeliveryExchange.Decision.ABORT) {
                return;
            }
            offsets.arm(prepared);
            markBatchProcessed(events, committer);
            // Debezium's markBatchFinished may internally turn a failed store
            // Future into a non-throwing "not committed" result. ACK is only
            // successful after our backing store observed the exact preview.
            offsets.requireCommitted(prepared);
        }
        catch (InterruptedException | RuntimeException | Error error) {
            deliveryFailure = error;
            throw error;
        }
        finally {
            offsets.disarm(prepared);
            exchange.settle(delivery, deliveryFailure);
        }
    }

    static <R> void markBatchProcessed(
            List<R> records, DebeziumEngine.RecordCommitter<R> committer)
            throws InterruptedException {
        for (R record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private void runEngine() {
        try {
            engine.run();
        }
        catch (Throwable error) {
            failure = error;
            completionMessage = "engine thread failed";
            state.set(State.FAILED);
        }
        finally {
            state.updateAndGet(current -> current == State.FAILED
                    ? current
                    : State.STOPPED);
            // Publish the terminal state before waking a timed poll, so a
            // wake-up caused by shutdown can never look like ordinary idle.
            exchange.close();
        }
    }

    private void abandonInBackground(Runnable reclaim) {
        Throwable cleanupFailure = null;
        try {
            if (!stop(Long.MAX_VALUE)) {
                throw new IllegalStateException(
                        "Debezium engine did not stop before the abandonment deadline");
            }
        }
        catch (Throwable error) {
            cleanupFailure = error;
        }

        Thread thread = engineThread;
        if (thread == null || !thread.isAlive()) {
            try {
                reclaim.run();
            }
            catch (Throwable error) {
                if (cleanupFailure != null) {
                    error.addSuppressed(cleanupFailure);
                }
                cleanupFailure = error;
            }
        }
        if (cleanupFailure != null) {
            failure = cleanupFailure;
            completionMessage = "abandoned connector cleanup failed";
            state.compareAndSet(State.STOPPING, State.FAILED);
        }
    }

    private void closeAndJoinEngine() throws Throwable {
        Thread thread = engineThread;
        while (thread != null && thread.isAlive()) {
            try {
                engine.close();
                break;
            }
            catch (IllegalStateException error) {
                String message = error.getMessage();
                if (message != null && message.contains("tasks are starting")) {
                    Thread.sleep(10);
                    continue;
                }
                if (message != null && (message.contains("already being")
                        || message.contains("already shut down"))) {
                    break;
                }
                throw error;
            }
        }
        if (thread != null && thread != Thread.currentThread()) {
            thread.join();
        }
        if (state.get() != State.FAILED) {
            state.set(State.STOPPED);
        }
    }

    private void completed(boolean success, String message, Throwable error) {
        completionMessage = message;
        if (!success) {
            failure = error == null ? new IllegalStateException(message) : error;
            state.set(State.FAILED);
        }
    }

    private void requireNotDisposed() {
        if (state.get() == State.DISPOSED) {
            throw new IllegalStateException("connector is disposed");
        }
    }

    private void requireRunningForPoll() {
        State observed = state.get();
        if (observed == State.FAILED) {
            throw new IllegalStateException("Debezium connector failed", failure);
        }
        if (observed != State.RUNNING) {
            throw new IllegalStateException(
                    "connector must be running before poll; current state is " + stateName());
        }
    }

    private String stateName() {
        return state.get().name().toLowerCase(java.util.Locale.ROOT);
    }

    private static String describe(Throwable error) {
        String message = error.getMessage();
        return error.getClass().getName() + (message == null ? "" : ": " + message);
    }
}
