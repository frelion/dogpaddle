package dev.dogpaddle.experiments.debeziumd1;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

final class DeliveryExchange<R> {
    enum Decision {
        ACK,
        ABORT
    }

    record Snapshot(Long token, int eventCount) {
        boolean hasOutstanding() {
            return token != null;
        }
    }

    static final class Delivery<R> {
        private final long token;
        private final List<R> records;
        private final byte[] json;
        private final CompletableFuture<Decision> decision = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();

        Delivery(long token, List<R> records, byte[] json) {
            this.token = token;
            this.records = List.copyOf(records);
            this.json = json.clone();
        }

        long token() {
            return token;
        }

        List<R> records() {
            return records;
        }

        byte[] json() {
            return json.clone();
        }
    }

    private Delivery<R> outstanding;
    private boolean closed;

    synchronized Delivery<R> install(long token, List<R> records, byte[] json)
            throws InterruptedException {
        while (outstanding != null && !closed) {
            wait();
        }
        if (closed) {
            throw new InterruptedException("delivery exchange is stopping");
        }
        outstanding = new Delivery<>(token, records, json);
        notifyAll();
        return outstanding;
    }

    synchronized Delivery<R> poll(long timeoutMillis) throws InterruptedException {
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long deadline = System.nanoTime() + remainingNanos;
        while (outstanding == null && !closed && remainingNanos > 0) {
            long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            int nanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(millis));
            wait(millis, nanos);
            remainingNanos = deadline - System.nanoTime();
        }
        return outstanding;
    }

    Decision awaitDecision(Delivery<R> delivery) throws InterruptedException {
        try {
            return delivery.decision.get();
        }
        catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("delivery decision failed", e.getCause());
        }
    }

    void ack(long token) {
        final Delivery<R> delivery;
        synchronized (this) {
            delivery = outstanding;
            if (delivery == null) {
                throw new IllegalStateException("there is no outstanding delivery");
            }
            if (delivery.token != token) {
                throw new IllegalArgumentException(
                        "ACK token " + token + " does not match outstanding token " + delivery.token);
            }
            if (!delivery.decision.complete(Decision.ACK)) {
                throw new IllegalStateException("delivery " + token + " has already been decided");
            }
        }
        try {
            delivery.settled.join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Debezium failed while acknowledging delivery " + token, cause);
        }
    }

    synchronized Snapshot snapshot() {
        return outstanding == null
                ? new Snapshot(null, 0)
                : new Snapshot(outstanding.token, outstanding.records.size());
    }

    synchronized void close() {
        closed = true;
        if (outstanding != null) {
            outstanding.decision.complete(Decision.ABORT);
        }
        notifyAll();
    }

    void settle(Delivery<R> delivery, Throwable failure) {
        synchronized (this) {
            if (outstanding == delivery) {
                outstanding = null;
                notifyAll();
            }
        }
        if (failure == null) {
            delivery.settled.complete(null);
        }
        else {
            delivery.settled.completeExceptionally(failure);
        }
    }
}
