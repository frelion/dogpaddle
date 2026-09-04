package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class ConnectorRuntimeTest {
    @Test
    void bridge_protocol_version_is_available_without_creating_a_connector() {
        assertEquals(1, DebeziumBridge.protocolVersion());
    }

    @Test
    void required_runtime_resources_are_available() throws Exception {
        DebeziumBridge.verifyRuntime();
    }

    @Test
    void create_rejects_a_delivery_bound_below_the_protocol_minimum() {
        byte[] configuration = ("{\"name\":\"minimum-bound\","
                + "\"connector.class\":\"example.MissingConnector\"}")
                .getBytes(StandardCharsets.UTF_8);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ConnectorRuntime.create(
                        configuration,
                        null,
                        DeliveryCodec.MINIMUM_MAXIMUM_BYTES - 1));

        assertEquals("maximum delivery bytes must be at least 68", error.getMessage());
    }

    @Test
    void create_rejects_a_bound_that_cannot_frame_the_connector_checkpoint() {
        byte[] configuration = ("{\"name\":\"orders\","
                + "\"connector.class\":\"example.Connector\"}")
                .getBytes(StandardCharsets.UTF_8);

        IllegalArgumentException empty = assertThrows(
                IllegalArgumentException.class,
                () -> ConnectorRuntime.create(configuration, null, 88));
        assertEquals(
                "maximum delivery bytes must be at least 89 for the initial checkpoint",
                empty.getMessage());

        Checkpoint restored = new Checkpoint(
                "orders",
                "example.Connector",
                java.util.Map.of(
                        new RawBytes(new byte[] {1}),
                        new RawBytes(new byte[] {2})));
        byte[] checkpoint = CheckpointCodec.encode(restored);
        int required = DeliveryCodec.MINIMUM_BYTES_EXCLUDING_CHECKPOINT + checkpoint.length;
        IllegalArgumentException existing = assertThrows(
                IllegalArgumentException.class,
                () -> ConnectorRuntime.create(
                        configuration, checkpoint, required - 1));
        assertEquals(
                "maximum delivery bytes must be at least " + required
                        + " for the initial checkpoint",
                existing.getMessage());
    }

    @Test
    void poll_is_idle_only_for_a_running_connector() {
        byte[] configuration = ("{\"name\":\"poll-state\",\"connector.class\":\""
                + ReclamationTestConnector.class.getName() + "\"}")
                .getBytes(StandardCharsets.UTF_8);
        ConnectorRuntime runtime = ConnectorRuntime.create(
                configuration, null, 1024);
        try {
            IllegalStateException created = assertThrows(
                    IllegalStateException.class,
                    () -> runtime.poll(0));
            assertEquals(
                    "connector must be running before poll; current state is created",
                    created.getMessage());

            runtime.stop(0);
            IllegalStateException stopped = assertThrows(
                    IllegalStateException.class,
                    () -> runtime.poll(0));
            assertEquals(
                    "connector must be running before poll; current state is stopped",
                    stopped.getMessage());
        }
        finally {
            runtime.dispose();
        }
    }

    @Test
    void shutdown_timeout_is_retryable_and_shutdown_failure_is_not_a_timeout() {
        CountDownLatch release = new CountDownLatch(1);
        ShutdownWorker delayed = new ShutdownWorker("delayed-shutdown", release::await);

        assertFalse(delayed.awaitUntil(System.nanoTime(), 0));
        release.countDown();
        assertTrue(delayed.awaitUntil(System.nanoTime(), Long.MAX_VALUE));

        ShutdownWorker failed = new ShutdownWorker(
                "failed-shutdown",
                () -> {
                    throw new IllegalStateException("shutdown failed");
                });
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> failed.awaitUntil(System.nanoTime(), Long.MAX_VALUE));
        assertEquals("shutdown failed", error.getMessage());
    }
}
