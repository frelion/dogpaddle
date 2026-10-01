package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DeliveryExchangeTest {
    @Test
    void one_delivery_is_stable_until_ack_settles_on_the_handler_thread() throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        byte[] encoded = new byte[] {1, 2, 3};
        DeliveryExchange.Delivery delivery = exchange.install(encoded);
        byte[] borrowed = exchange.poll(0).encoded();
        assertSame(encoded, borrowed);
        assertSame(borrowed, exchange.poll(0).encoded());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> ack = executor.submit(() -> exchange.ack(Long.MAX_VALUE));

            assertEquals(DeliveryExchange.Decision.ACK, exchange.awaitDecision(delivery));
            assertFalse(ack.isDone(), "ACK returned before the handler settled");
            exchange.settle(delivery, null);
            assertTrue(ack.get(1, TimeUnit.SECONDS));
            assertNull(exchange.poll(0));
            assertSame(borrowed, delivery.encoded());
            assertArrayEquals(new byte[] {1, 2, 3}, borrowed);

            assertThrows(
                    IllegalStateException.class,
                    () -> exchange.ack(Long.MAX_VALUE));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void close_aborts_the_outstanding_delivery()
            throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        DeliveryExchange.Delivery delivery = exchange.install(new byte[] {4});
        byte[] borrowed = exchange.poll(0).encoded();

        exchange.close();
        assertEquals(DeliveryExchange.Decision.ABORT, exchange.awaitDecision(delivery));
        assertNull(exchange.poll(0));
        exchange.settle(delivery, null);
        assertSame(borrowed, delivery.encoded());
        assertArrayEquals(new byte[] {4}, borrowed);
    }

    @Test
    void ack_timeout_then_close_preserves_ack_and_handler_can_settle() throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        DeliveryExchange.Delivery delivery = exchange.install(new byte[] {5});
        byte[] borrowed = exchange.poll(0).encoded();

        assertFalse(exchange.ack(0));
        exchange.close();
        assertEquals(DeliveryExchange.Decision.ACK, exchange.awaitDecision(delivery));
        assertNull(exchange.poll(0));

        exchange.settle(delivery, null);
        assertNull(exchange.poll(0));
        assertSame(borrowed, delivery.encoded());
        assertArrayEquals(new byte[] {5}, borrowed);
    }
}
