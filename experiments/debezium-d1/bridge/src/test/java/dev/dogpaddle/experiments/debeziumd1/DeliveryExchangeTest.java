package dev.dogpaddle.experiments.debeziumd1;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class DeliveryExchangeTest {
    @Test
    void repeated_polls_return_the_same_outstanding_bytes_until_ack() throws Exception {
        DeliveryExchange<String> exchange = new DeliveryExchange<>();
        assertEquals(new DeliveryExchange.Snapshot(null, 0), exchange.snapshot());
        DeliveryExchange.Delivery<String> delivery =
                exchange.install(7, List.of("one", "two"), new byte[] { 1, 2, 3 });

        assertEquals(new DeliveryExchange.Snapshot(7L, 2), exchange.snapshot());
        assertEquals(7, exchange.poll(0).token());
        assertArrayEquals(new byte[] { 1, 2, 3 }, exchange.poll(0).json());
        assertEquals(DeliveryExchange.Decision.ACK, acknowledge(exchange, delivery));
        assertNull(exchange.poll(0));
        assertEquals(new DeliveryExchange.Snapshot(null, 0), exchange.snapshot());
    }

    @Test
    void ack_waits_until_the_handler_settles_the_delivery() throws Exception {
        DeliveryExchange<String> exchange = new DeliveryExchange<>();
        DeliveryExchange.Delivery<String> delivery =
                exchange.install(11, List.of("record"), new byte[] { 4 });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> ack = CompletableFuture.runAsync(() -> exchange.ack(11), executor);
            assertEquals(DeliveryExchange.Decision.ACK, exchange.awaitDecision(delivery));
            assertFalse(ack.isDone());

            exchange.settle(delivery, null);

            ack.orTimeout(Duration.ofSeconds(1).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                    .join();
            assertTrue(ack.isDone());
        }
        finally {
            shutdown(executor);
        }
    }

    private static DeliveryExchange.Decision acknowledge(
            DeliveryExchange<String> exchange,
            DeliveryExchange.Delivery<String> delivery) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> ack = CompletableFuture.runAsync(() -> exchange.ack(delivery.token()), executor);
            DeliveryExchange.Decision decision;
            try {
                decision = exchange.awaitDecision(delivery);
            }
            catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            exchange.settle(delivery, null);
            ack.join();
            return decision;
        }
        finally {
            shutdown(executor);
        }
    }

    private static void shutdown(ExecutorService executor) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("executor did not terminate");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while stopping executor", e);
        }
    }
}
