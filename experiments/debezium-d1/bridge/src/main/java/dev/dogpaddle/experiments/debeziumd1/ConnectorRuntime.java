package dev.dogpaddle.experiments.debeziumd1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.kafka.connect.source.SourceRecord;

final class ConnectorRuntime {
    private enum State {
        CREATED,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        FAILED
    }

    private final ObjectMapper mapper;
    private final DeliveryCodec codec;
    private final DeliveryExchange<RecordChangeEvent<SourceRecord>> exchange;
    private final LongSupplier nextToken;
    private final AtomicReference<State> state;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private final ShutdownWorker shutdownWorker;
    private final String jvmId;
    private final long javaProcessId;

    private volatile Thread engineThread;
    private volatile String completionMessage;
    private volatile Throwable failure;
    private volatile long lastAckToken;

    private ConnectorRuntime(
            Properties properties,
            LongSupplier nextToken,
            String jvmId,
            long javaProcessId) {
        this.mapper = new ObjectMapper();
        this.codec = new DeliveryCodec(mapper);
        this.exchange = new DeliveryExchange<>();
        this.nextToken = nextToken;
        this.state = new AtomicReference<>(State.CREATED);
        this.jvmId = jvmId;
        this.javaProcessId = javaProcessId;

        DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> consumer = this::handleBatch;
        DebeziumEngine.Builder<RecordChangeEvent<SourceRecord>> builder =
                DebeziumEngine.create(ChangeEventFormat.of(Connect.class));

        // notifying() reads the engine configuration in 3.6.2, so using(Properties)
        // must precede it. Every call here is part of Debezium's public Engine API.
        builder.using(properties);
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

        // ACK is a durability boundary in this experiment. Avoid interval-based
        // ambiguity: markBatchFinished always asks the offset store to flush.
        builder.using(OffsetCommitPolicy.always());
        this.engine = builder.build();
        this.shutdownWorker = new ShutdownWorker(
                "dogpaddle-debezium-d1-shutdown", this::closeAndJoinEngine);
    }

    static ConnectorRuntime create(
            byte[] configurationJson,
            LongSupplier nextToken,
            String jvmId,
            long javaProcessId) {
        ObjectMapper mapper = new ObjectMapper();
        Properties properties = ConnectorConfiguration.parse(configurationJson, mapper);
        return new ConnectorRuntime(properties, nextToken, jvmId, javaProcessId);
    }

    synchronized void start() {
        if (!state.compareAndSet(State.CREATED, State.STARTING)) {
            throw new IllegalStateException(
                    "engine can only start from created state; current state is " + stateName());
        }

        Thread thread = new Thread(this::runEngine, "dogpaddle-debezium-d1-engine");
        thread.setDaemon(false);
        engineThread = thread;
        thread.start();
    }

    byte[] poll(long timeoutMillis, int maxBytes) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("poll timeout must be non-negative");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("poll maxBytes must be positive");
        }
        try {
            DeliveryExchange.Delivery<RecordChangeEvent<SourceRecord>> delivery =
                    exchange.poll(timeoutMillis);
            if (delivery == null) {
                return null;
            }
            byte[] json = delivery.json();
            if (json.length > maxBytes) {
                throw new IllegalStateException(
                        "delivery " + delivery.token() + " is " + json.length
                                + " bytes, exceeding poll maxBytes=" + maxBytes);
            }
            return json;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("poll interrupted", e);
        }
    }

    void ack(long token) {
        exchange.ack(token);
        lastAckToken = token;
    }

    void stop(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("stop timeout must be non-negative");
        }

        synchronized (this) {
            State observed = state.get();
            if (observed == State.STOPPED) {
                return;
            }
            if (observed == State.CREATED) {
                exchange.close();
                state.set(State.STOPPED);
                return;
            }

            State transitioned = state.updateAndGet(current ->
                    current == State.FAILED || current == State.STOPPED
                            ? current
                            : State.STOPPING);
            exchange.close();
            if (transitioned == State.STOPPED) {
                return;
            }
        }

        shutdownWorker.await(timeoutMillis);
    }

    byte[] status() {
        DeliveryExchange.Snapshot exchangeSnapshot = exchange.snapshot();
        ObjectNode root = mapper.createObjectNode();
        root.put("protocol", 1);
        root.put("kind", "status");
        root.put("state", stateName());
        root.put("jvm_id", jvmId);
        root.put("java_process_id", javaProcessId);
        root.put("outstanding", exchangeSnapshot.hasOutstanding());
        root.put("event_count", exchangeSnapshot.eventCount());
        Long outstandingToken = exchangeSnapshot.token();
        if (outstandingToken == null) {
            root.putNull("token");
        }
        else {
            root.put("token", outstandingToken);
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
        try {
            return mapper.writeValueAsBytes(root);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode runtime status", e);
        }
    }

    private void handleBatch(
            List<RecordChangeEvent<SourceRecord>> records,
            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
            throws InterruptedException {
        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        List<SourceRecord> sourceRecords = records.stream()
                .map(RecordChangeEvent::record)
                .toList();
        if (sourceRecords.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalStateException("Debezium emitted a null SourceRecord");
        }

        long token = nextToken.getAsLong();
        byte[] json = codec.encode(token, sourceRecords);
        DeliveryExchange.Delivery<RecordChangeEvent<SourceRecord>> delivery =
                exchange.install(token, records, json);

        Throwable deliveryFailure = null;
        try {
            if (exchange.awaitDecision(delivery) == DeliveryExchange.Decision.ABORT) {
                return;
            }
            // SourceRecordCommitter is intentionally invoked only on this handler
            // thread. It is not thread-safe in Debezium 3.6.2.
            markBatchProcessed(delivery.records(), committer);
        }
        catch (InterruptedException | RuntimeException | Error e) {
            deliveryFailure = e;
            throw e;
        }
        finally {
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
            exchange.close();
            state.updateAndGet(current -> current == State.FAILED ? current : State.STOPPED);
        }
    }

    private void closeAndJoinEngine() throws Throwable {
        Throwable closeFailure = null;
        try {
            engine.close();
        }
        catch (Throwable error) {
            closeFailure = error;
        }

        try {
            Thread thread = engineThread;
            if (thread != null && thread != Thread.currentThread()) {
                thread.join();
            }
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            if (closeFailure != null) {
                error.addSuppressed(closeFailure);
            }
            failure = error;
            completionMessage = "engine shutdown was interrupted";
            state.set(State.FAILED);
            throw error;
        }

        if (closeFailure != null) {
            failure = closeFailure;
            completionMessage = "engine shutdown failed";
            state.set(State.FAILED);
            throw closeFailure;
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

    private String stateName() {
        return state.get().name().toLowerCase(java.util.Locale.ROOT);
    }

    private static String describe(Throwable error) {
        String message = error.getMessage();
        return error.getClass().getName() + (message == null ? "" : ": " + message);
    }
}
