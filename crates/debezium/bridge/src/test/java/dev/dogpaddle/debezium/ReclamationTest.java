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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ReclamationTest {
    private static final long LIFECYCLE_TIMEOUT_MILLIS = 5_000;

    @Test
    void abandon_is_non_blocking_idempotent_and_reclaims_a_created_runtime()
            throws Exception {
        ConnectorRuntime runtime = create("abandon-created");

        runtime.abandon();
        runtime.abandon();

        awaitReclaimed(runtime);
        ConnectorRuntime replacement = create("abandon-created");
        replacement.stop(0);
        replacement.dispose();
    }

    @Test
    void creation_failure_cleanup_releases_the_reserved_engine_name() {
        ConnectorRuntime runtime = create("global-reference-failure");
        runtime.discardCreated();
        assertTrue(runtime.isDisposed());

        ConnectorRuntime replacement = create("global-reference-failure");
        replacement.stop(0);
        replacement.dispose();
    }

    @Test
    void explicit_stop_dispose_can_race_abandon_without_double_unregister()
            throws Exception {
        ConnectorRuntime runtime = create("abandon-race");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var abandoned = executor.submit(() -> {
                await(start);
                runtime.abandon();
            });
            var explicit = executor.submit(() -> {
                await(start);
                try {
                    runtime.stop(1_000);
                }
                catch (IllegalStateException ignored) {
                    // The asynchronous reclaimer disposed the runtime first.
                }
                runtime.dispose();
            });
            start.countDown();
            abandoned.get(2, TimeUnit.SECONDS);
            explicit.get(2, TimeUnit.SECONDS);
        }
        finally {
            executor.shutdownNow();
        }

        awaitReclaimed(runtime);
        ConnectorRuntime replacement = create("abandon-race");
        replacement.stop(0);
        replacement.dispose();
    }

    @Test
    void stopping_and_disposing_one_running_runtime_does_not_disturb_another()
            throws Exception {
        String firstName = "running-isolation-first";
        String secondName = "running-isolation-second";
        ReclamationTestConnector.Control firstControl =
                ReclamationTestConnector.install(firstName, false, false, false);
        ReclamationTestConnector.Control secondControl =
                ReclamationTestConnector.install(secondName, false, false, false);
        ConnectorRuntime first = null;
        ConnectorRuntime second = null;
        try {
            first = create(firstName);
            second = create(secondName);
            assertTrue(first.start(LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(second.start(LIFECYCLE_TIMEOUT_MILLIS));
            awaitRunning(firstControl);
            awaitRunning(secondControl);

            assertTrue(first.stop(LIFECYCLE_TIMEOUT_MILLIS));
            first.dispose();
            assertTrue(first.isDisposed());

            assertNull(second.poll(0));
            assertTrue(second.stop(LIFECYCLE_TIMEOUT_MILLIS));
            second.dispose();
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
        ConnectorRuntime failed = null;
        ConnectorRuntime replacement = null;
        try {
            failed = create(name);
            ConnectorRuntime failedRuntime = failed;
            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> failedRuntime.start(LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(error.getMessage().contains("deliberate connector startup failure"));

            failed.abandon();
            awaitReclaimed(failed);
            replacement = create(name);
            assertTrue(replacement.stop(0));
            replacement.dispose();
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
        ConnectorRuntime timedOut = null;
        ConnectorRuntime replacement = null;
        try {
            timedOut = create(name);
            assertFalse(timedOut.start(0));

            timedOut.abandon();
            control.releaseStartup();
            awaitReclaimed(timedOut);

            replacement = create(name);
            assertTrue(replacement.stop(0));
            replacement.dispose();
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
        ConnectorRuntime runtime = null;
        try {
            runtime = create(name);
            assertTrue(runtime.start(LIFECYCLE_TIMEOUT_MILLIS));
            awaitRunning(control);

            assertFalse(runtime.stop(0));
            assertTrue(control.awaitTaskStopStarted(LIFECYCLE_TIMEOUT_MILLIS));

            control.releaseTaskStop();
            assertTrue(runtime.stop(LIFECYCLE_TIMEOUT_MILLIS));
            assertTrue(runtime.stop(0));
            runtime.dispose();
        }
        finally {
            control.releaseTaskStop();
            try {
                cleanup(runtime);
            }
            finally {
                ReclamationTestConnector.uninstall(name, control);
            }
        }
    }

    @Test
    void abandonment_keeps_ownership_after_close_failure_until_engine_exits()
            throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        Thread engine = new Thread(() -> await(release), "eventually-stopped-engine");
        engine.start();
        AtomicBoolean reclaimed = new AtomicBoolean();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var cleanup = executor.submit(() -> ConnectorRuntime.awaitTerminationAndReclaim(
                    engine,
                    () -> reclaimed.set(true),
                    new IllegalStateException("close failed")));
            assertFalse(cleanup.isDone());
            assertFalse(reclaimed.get());

            release.countDown();
            Throwable failure = cleanup.get(LIFECYCLE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertTrue(reclaimed.get());
            assertTrue(failure instanceof IllegalStateException);
        }
        finally {
            release.countDown();
            engine.join(LIFECYCLE_TIMEOUT_MILLIS);
            executor.shutdownNow();
        }
    }

    private static ConnectorRuntime create(String name) {
        String json = "{\"name\":\"" + name + "\",\"connector.class\":\""
                + ReclamationTestConnector.class.getName() + "\"}";
        return ConnectorRuntime.create(
                json.getBytes(StandardCharsets.UTF_8), null, 1024);
    }

    private static void awaitReclaimed(ConnectorRuntime runtime) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(LIFECYCLE_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            if (runtime.isDisposed()) {
                assertDoesNotThrow(runtime::dispose);
                return;
            }
            Thread.sleep(5);
        }
        assertTrue(runtime.isDisposed());
    }

    private static void awaitRunning(ReclamationTestConnector.Control control) throws Exception {
        assertTrue(control.awaitPollStarted(LIFECYCLE_TIMEOUT_MILLIS));
    }

    private static void cleanup(ConnectorRuntime runtime) throws InterruptedException {
        if (runtime == null || runtime.isDisposed()) {
            return;
        }
        try {
            if (runtime.stop(LIFECYCLE_TIMEOUT_MILLIS)) {
                runtime.dispose();
                return;
            }
        }
        catch (IllegalStateException error) {
            if (runtime.isDisposed()) {
                return;
            }
        }
        runtime.abandon();
        awaitReclaimed(runtime);
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
