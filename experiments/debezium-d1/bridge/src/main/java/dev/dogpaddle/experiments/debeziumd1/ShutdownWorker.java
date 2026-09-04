package dev.dogpaddle.experiments.debeziumd1;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs one potentially blocking shutdown action without trapping the JNI caller. */
final class ShutdownWorker {
    @FunctionalInterface
    interface Action {
        void run() throws Throwable;
    }

    private final String threadName;
    private final Action action;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    private Thread thread;

    ShutdownWorker(String threadName, Action action) {
        this.threadName = threadName;
        this.action = action;
    }

    synchronized void start() {
        if (thread != null) {
            return;
        }
        Thread created = new Thread(this::run, threadName);
        created.setDaemon(true);
        thread = created;
        created.start();
    }

    void await(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("shutdown timeout must be non-negative");
        }
        start();
        try {
            completion.get(timeoutMillis, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            throw new IllegalStateException(
                    "Debezium engine did not stop within " + Duration.ofMillis(timeoutMillis), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("stop interrupted", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Debezium engine shutdown failed", cause);
        }
    }

    private void run() {
        try {
            action.run();
            completion.complete(null);
        }
        catch (Throwable error) {
            completion.completeExceptionally(error);
        }
    }
}
