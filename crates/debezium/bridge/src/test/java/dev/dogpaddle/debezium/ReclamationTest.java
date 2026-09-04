package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ReclamationTest {
    private static final long LIFECYCLE_TIMEOUT_MILLIS = 5_000;

    @Test
    void abandon_is_non_blocking_idempotent_and_reclaims_a_created_handle()
            throws Exception {
        long handle = create("abandon-created");

        DebeziumBridge.abandon(handle);
        DebeziumBridge.abandon(handle);

        awaitReclaimed(handle);
        long replacement = create("abandon-created");
        DebeziumBridge.stop(replacement, 0);
        DebeziumBridge.dispose(replacement);
    }

    @Test
    void explicit_stop_dispose_can_race_abandon_without_double_unregister()
            throws Exception {
        long handle = create("abandon-race");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var abandoned = executor.submit(() -> {
                await(start);
                DebeziumBridge.abandon(handle);
            });
            var explicit = executor.submit(() -> {
                await(start);
                try {
                    DebeziumBridge.stop(handle, 1_000);
                }
                catch (IllegalArgumentException ignored) {
                    // The asynchronous reclaimer won the handle race.
                }
                DebeziumBridge.dispose(handle);
            });
            start.countDown();
            abandoned.get(2, TimeUnit.SECONDS);
            explicit.get(2, TimeUnit.SECONDS);
        }
        finally {
            executor.shutdownNow();
        }

        awaitReclaimed(handle);
        long replacement = create("abandon-race");
        DebeziumBridge.stop(replacement, 0);
        DebeziumBridge.dispose(replacement);
    }

    @Test
    void stopping_and_disposing_one_running_handle_does_not_disturb_another()
            throws Exception {
        String firstName = "running-isolation-first";
        String secondName = "running-isolation-second";
        ReclamationTestConnector.Control firstControl =
                ReclamationTestConnector.install(firstName, false, false, false);
        ReclamationTestConnector.Control secondControl =
                ReclamationTestConnector.install(secondName, false, false, false);
        long first = 0;
        long second = 0;
        try {
            first = create(firstName);
            second = create(secondName);
            assertTrue(DebeziumBridge.start(first, LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(DebeziumBridge.start(second, LIFECYCLE_TIMEOUT_MILLIS));
            awaitRunning(firstControl);
            awaitRunning(secondControl);

            assertTrue(DebeziumBridge.stop(first, LIFECYCLE_TIMEOUT_MILLIS));
            DebeziumBridge.dispose(first);
            long disposedFirst = first;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> DebeziumBridge.failureKind(disposedFirst));

            assertNull(DebeziumBridge.poll(second, 0));
            assertTrue(DebeziumBridge.stop(second, LIFECYCLE_TIMEOUT_MILLIS));
            DebeziumBridge.dispose(second);
        }
        finally {
            firstControl.releaseTaskStop();
            secondControl.releaseTaskStop();
            try {
                cleanup(first);
            }
            finally {
                try {
                    cleanup(second);
                }
                finally {
                    ReclamationTestConnector.uninstall(firstName, firstControl);
                    ReclamationTestConnector.uninstall(secondName, secondControl);
                }
            }
        }
    }

    @Test
    void connector_startup_failure_terminates_and_reclaims_the_runtime()
            throws Exception {
        String name = "startup-failure-reclamation";
        ReclamationTestConnector.Control control =
                ReclamationTestConnector.install(name, true, false, false);
        long failed = 0;
        long replacement = 0;
        try {
            failed = create(name);
            long failedHandle = failed;
            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> DebeziumBridge.start(
                            failedHandle, LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(error.getMessage().contains("deliberate connector startup failure"));

            DebeziumBridge.abandon(failed);
            awaitReclaimed(failed);
            replacement = create(name);
            assertTrue(DebeziumBridge.stop(replacement, 0));
            DebeziumBridge.dispose(replacement);
        }
        finally {
            control.releaseTaskStop();
            try {
                cleanup(failed);
            }
            finally {
                try {
                    cleanup(replacement);
                }
                finally {
                    ReclamationTestConnector.uninstall(name, control);
                }
            }
        }
    }

    @Test
    void startup_timeout_can_be_abandoned_and_releases_the_engine_name()
            throws Exception {
        String name = "startup-timeout-reclamation";
        ReclamationTestConnector.Control control =
                ReclamationTestConnector.install(name, false, true, false);
        long timedOut = 0;
        long replacement = 0;
        try {
            timedOut = create(name);
            assertFalse(DebeziumBridge.start(timedOut, 0));

            DebeziumBridge.abandon(timedOut);
            control.releaseStartup();
            awaitReclaimed(timedOut);

            replacement = create(name);
            assertTrue(DebeziumBridge.stop(replacement, 0));
            DebeziumBridge.dispose(replacement);
        }
        finally {
            control.releaseStartup();
            control.releaseTaskStop();
            try {
                cleanup(timedOut);
            }
            finally {
                try {
                    cleanup(replacement);
                }
                finally {
                    ReclamationTestConnector.uninstall(name, control);
                }
            }
        }
    }

    @Test
    void running_stop_timeout_is_retryable_and_repeated_stop_succeeds()
            throws Exception {
        String name = "running-stop-retry";
        ReclamationTestConnector.Control control =
                ReclamationTestConnector.install(name, false, false, true);
        long handle = 0;
        try {
            handle = create(name);
            assertTrue(DebeziumBridge.start(handle, LIFECYCLE_TIMEOUT_MILLIS));
            awaitRunning(control);

            assertFalse(DebeziumBridge.stop(handle, 0));
            assertTrue(control.awaitTaskStopStarted(LIFECYCLE_TIMEOUT_MILLIS));

            control.releaseTaskStop();
            assertTrue(DebeziumBridge.stop(handle, LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(DebeziumBridge.stop(handle, 0));
            DebeziumBridge.dispose(handle);
        }
        finally {
            control.releaseTaskStop();
            try {
                cleanup(handle);
            }
            finally {
                ReclamationTestConnector.uninstall(name, control);
            }
        }
    }

    private static long create(String name) {
        String json = "{\"name\":\"" + name + "\",\"connector.class\":\""
                + ReclamationTestConnector.class.getName() + "\"}";
        return DebeziumBridge.create(
                json.getBytes(StandardCharsets.UTF_8), null, 1024);
    }

    private static void awaitReclaimed(long handle) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(LIFECYCLE_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            try {
                DebeziumBridge.failureKind(handle);
            }
            catch (IllegalArgumentException expected) {
                assertDoesNotThrow(() -> DebeziumBridge.dispose(handle));
                return;
            }
            Thread.sleep(5);
        }
        assertThrows(IllegalArgumentException.class, () -> DebeziumBridge.failureKind(handle));
    }

    private static void awaitRunning(ReclamationTestConnector.Control control) throws Exception {
        assertTrue(control.awaitPollStarted(LIFECYCLE_TIMEOUT_MILLIS));
    }

    private static void cleanup(long handle) throws InterruptedException {
        if (handle == 0) {
            return;
        }
        try {
            if (DebeziumBridge.stop(handle, LIFECYCLE_TIMEOUT_MILLIS)) {
                DebeziumBridge.dispose(handle);
                return;
            }
        }
        catch (IllegalArgumentException ignored) {
            return;
        }
        DebeziumBridge.abandon(handle);
        awaitReclaimed(handle);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        }
    }
}
