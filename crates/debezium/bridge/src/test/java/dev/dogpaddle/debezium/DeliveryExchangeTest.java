package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class DeliveryExchangeTest {
    @Test
    void one_delivery_is_stable_until_ack_settles_on_the_handler_thread() throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        DeliveryExchange.Delivery delivery = exchange.install(
                7,
                List.of(record()),
                prepared(),
                new byte[] {1, 2, 3});
        assertArrayEquals(delivery.encoded(), exchange.poll(0).encoded());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> ack = executor.submit(() -> exchange.ack(7, Long.MAX_VALUE));

            assertEquals(DeliveryExchange.Decision.ACK, exchange.awaitDecision(delivery));
            assertFalse(ack.isDone(), "ACK returned before the handler settled");
            exchange.settle(delivery, null);
            assertTrue(ack.get(1, TimeUnit.SECONDS));

            assertFalse(exchange.snapshot().hasOutstanding());
            assertThrows(
                    IllegalStateException.class,
                    () -> exchange.ack(7, Long.MAX_VALUE));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void wrong_token_preserves_the_outstanding_delivery_and_close_aborts_it()
            throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        DeliveryExchange.Delivery delivery = exchange.install(
                11,
                List.of(record()),
                prepared(),
                new byte[] {4});

        assertThrows(IllegalArgumentException.class, () -> exchange.ack(12, 0));
        assertTrue(exchange.snapshot().hasOutstanding());
        exchange.close();
        assertEquals(DeliveryExchange.Decision.ABORT, exchange.awaitDecision(delivery));
        exchange.settle(delivery, null);
    }

    @Test
    void zero_deadline_returns_false_after_signaling_ack_and_handler_can_settle() throws Exception {
        DeliveryExchange exchange = new DeliveryExchange();
        DeliveryExchange.Delivery delivery = exchange.install(
                13,
                List.of(record()),
                prepared(),
                new byte[] {5});

        assertFalse(exchange.ack(13, 0));
        assertEquals(DeliveryExchange.Decision.ACK, exchange.awaitDecision(delivery));
        assertTrue(exchange.snapshot().hasOutstanding());

        exchange.settle(delivery, null);
        assertFalse(exchange.snapshot().hasOutstanding());
    }

    private static SourceRecord record() {
        return new SourceRecord(
                Map.of("partition", "a"),
                Map.of("position", 1L),
                "topic",
                Schema.STRING_SCHEMA,
                "value");
    }

    private static OffsetStoreRegistry.PreparedCheckpoint prepared() {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        return new OffsetStoreRegistry.PreparedCheckpoint(
                0, Map.of(), checkpoint, CheckpointCodec.encode(checkpoint));
    }
}
