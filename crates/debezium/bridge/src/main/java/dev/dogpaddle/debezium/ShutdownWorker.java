package dev.dogpaddle.debezium;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs one potentially blocking shutdown action outside the JNI caller. */
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

    boolean awaitUntil(long startedAt, long timeoutNanos) {
        if (timeoutNanos < 0) {
            throw new IllegalArgumentException("shutdown timeout must be non-negative");
        }
        start();
        try {
            long elapsed = System.nanoTime() - startedAt;
            long remaining = elapsed <= 0
                    ? timeoutNanos
                    : Math.max(0, timeoutNanos - elapsed);
            completion.get(remaining, TimeUnit.NANOSECONDS);
            return true;
        }
        catch (TimeoutException error) {
            return false;
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("stop interrupted", error);
        }
        catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
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
