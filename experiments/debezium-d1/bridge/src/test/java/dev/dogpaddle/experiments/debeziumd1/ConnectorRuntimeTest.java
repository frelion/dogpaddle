package dev.dogpaddle.experiments.debeziumd1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.engine.DebeziumEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ConnectorRuntimeTest {
    @Test
    void ack_marks_every_record_in_order_before_finishing_the_batch() throws Exception {
        List<String> calls = new ArrayList<>();
        DebeziumEngine.RecordCommitter<String> committer = new DebeziumEngine.RecordCommitter<>() {
            @Override
            public void markProcessed(String record) {
                calls.add("record:" + record);
            }

            @Override
            public void markBatchFinished() {
                calls.add("batch");
            }

            @Override
            public void markProcessed(String record, DebeziumEngine.Offsets sourceOffsets) {
                throw new AssertionError("updated offsets are not part of the D1 ACK protocol");
            }

            @Override
            public DebeziumEngine.Offsets buildOffsets() {
                throw new AssertionError("updated offsets are not part of the D1 ACK protocol");
            }
        };

        ConnectorRuntime.markBatchProcessed(List.of("a", "b", "c"), committer);

        assertEquals(List.of("record:a", "record:b", "record:c", "batch"), calls);
    }

    @Test
    void shutdown_worker_enforces_the_deadline_and_starts_its_action_once() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger starts = new AtomicInteger();
        AtomicBoolean daemon = new AtomicBoolean();
        ShutdownWorker worker = new ShutdownWorker("test-shutdown", () -> {
            starts.incrementAndGet();
            daemon.set(Thread.currentThread().isDaemon());
            entered.countDown();
            release.await();
        });

        worker.start();
        assertTrue(entered.await(1, TimeUnit.SECONDS), "shutdown action did not start");

        IllegalStateException timeout = assertThrows(
                IllegalStateException.class,
                () -> worker.await(0));
        assertTrue(timeout.getMessage().contains("did not stop within PT0S"));
        worker.start();
        assertEquals(1, starts.get());
        assertTrue(daemon.get());

        release.countDown();
        worker.await(TimeUnit.SECONDS.toMillis(1));
        assertEquals(1, starts.get());
    }

    @Test
    void delivery_tokens_are_monotonic_across_fresh_runtime_handles() {
        long firstHandleToken = D1Bridge.nextDeliveryToken();
        long freshHandleToken = D1Bridge.nextDeliveryToken();

        assertEquals(Math.addExact(firstHandleToken, 1), freshHandleToken);
    }

    @Test
    void delivery_token_exhaustion_is_permanent() {
        AtomicLong sequence = new AtomicLong(Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, D1Bridge.nextDeliveryToken(sequence));
        assertThrows(IllegalStateException.class, () -> D1Bridge.nextDeliveryToken(sequence));
        assertThrows(IllegalStateException.class, () -> D1Bridge.nextDeliveryToken(sequence));
    }
}
