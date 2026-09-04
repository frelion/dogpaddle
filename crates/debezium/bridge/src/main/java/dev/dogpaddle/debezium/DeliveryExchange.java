package dev.dogpaddle.debezium;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A single outstanding delivery with an ACK decision made by the Rust host. */
final class DeliveryExchange {
    enum Decision {
        ACK,
        ABORT
    }

    static final class Delivery {
        private final byte[] encoded;
        private final CompletableFuture<Decision> decision = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();

        Delivery(byte[] encoded) {
            this.encoded = encoded;
        }

        byte[] encoded() {
            return encoded.clone();
        }
    }

    private Delivery outstanding;
    private boolean closed;

    synchronized Delivery install(byte[] encoded) throws InterruptedException {
        while (outstanding != null && !closed) {
            wait();
        }
        if (closed) {
            throw new InterruptedException("delivery exchange is stopping");
        }
        outstanding = new Delivery(encoded);
        notifyAll();
        return outstanding;
    }

    synchronized Delivery poll(long timeoutMillis) throws InterruptedException {
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long deadline = System.nanoTime() + remainingNanos;
        while (outstanding == null && !closed && remainingNanos > 0) {
            long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            int nanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(millis));
            wait(millis, nanos);
            remainingNanos = deadline - System.nanoTime();
        }
        return closed ? null : outstanding;
    }

    Decision awaitDecision(Delivery delivery) throws InterruptedException {
        try {
            return delivery.decision.get();
        }
        catch (java.util.concurrent.ExecutionException error) {
            throw new IllegalStateException("delivery decision failed", error.getCause());
        }
    }

    boolean ack(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("ACK timeout must be non-negative");
        }
        long startedAt = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        final Delivery delivery;
        synchronized (this) {
            delivery = outstanding;
            if (delivery == null) {
                throw new IllegalStateException("there is no outstanding delivery");
            }
            if (!delivery.decision.complete(Decision.ACK)) {
                throw new IllegalStateException("delivery has already been decided");
            }
        }
        try {
            long elapsed = System.nanoTime() - startedAt;
            long remaining = elapsed <= 0
                    ? timeoutNanos
                    : Math.max(0, timeoutNanos - elapsed);
            delivery.settled.get(remaining, TimeUnit.NANOSECONDS);
            return true;
        }
        catch (TimeoutException error) {
            return false;
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("ACK interrupted", error);
        }
        catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IllegalStateException("Debezium failed while acknowledging delivery", cause);
        }
    }

    synchronized void close() {
        closed = true;
        if (outstanding != null) {
            outstanding.decision.complete(Decision.ABORT);
        }
        notifyAll();
    }

    void settle(Delivery delivery, Throwable failure) {
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
