package dev.dogpaddle.debezium;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.TrustManagerFactory;

/** The connector-neutral, pull-based JNI surface used by the Rust runtime. */
public final class DebeziumBridge {
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);
    private static final AtomicLong NEXT_DELIVERY_TOKEN = new AtomicLong(1);
    private static final Map<Long, ConnectorRuntime> RUNTIMES = new ConcurrentHashMap<>();

    private DebeziumBridge() {
    }

    /** Returns the JNI and wire protocol version without creating a connector. */
    public static int protocolVersion() {
        return 1;
    }

    /** Verifies the bundled runtime resources required by connector operation. */
    public static void verifyRuntime() throws GeneralSecurityException, UnknownHostException {
        if (!"UTF-8".equals(StandardCharsets.UTF_8.name())) {
            throw new IllegalStateException("UTF-8 charset is unavailable");
        }
        ZoneId.of("Asia/Shanghai").getRules().getOffset(Instant.EPOCH);
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init((KeyStore) null);
        InetAddress.getByName("localhost");
    }

    /** Creates a stopped connector from JSON properties and an optional checkpoint. */
    public static long create(
            byte[] configurationJson,
            byte[] checkpoint,
            int maximumDeliveryBytes) {
        long handle = nextPositive(NEXT_HANDLE, "connector handle");
        ConnectorRuntime runtime = ConnectorRuntime.create(
                configurationJson,
                checkpoint,
                maximumDeliveryBytes,
                DebeziumBridge::nextDeliveryToken);
        if (RUNTIMES.putIfAbsent(handle, runtime) != null) {
            throw new IllegalStateException("connector handle collision");
        }
        return handle;
    }

    /** Starts a connector handle exactly once. */
    public static void start(long handle) {
        runtime(handle).start();
    }

    /** Returns one encoded delivery or {@code null} on an ordinary timeout. */
    public static byte[] poll(long handle, long timeoutMillis) {
        return runtime(handle).poll(timeoutMillis);
    }

    /**
     * Acknowledges the exact outstanding delivery token.
     *
     * @return {@code true} after the handler and offset commit settle, or
     *         {@code false} when only the supplied deadline expires
     */
    public static boolean ack(long handle, long token, long timeoutMillis) {
        return runtime(handle).ack(token, timeoutMillis);
    }

    /**
     * Requests shutdown and waits no longer than the supplied total deadline.
     *
     * @return {@code true} after the engine thread is joined, or {@code false}
     *         when only the supplied deadline expires
     */
    public static boolean stop(long handle, long timeoutMillis) {
        return runtime(handle).stop(timeoutMillis);
    }

    /** Removes a fully stopped connector and its checkpoint registry entry. */
    public static void dispose(long handle) {
        ConnectorRuntime runtime = RUNTIMES.get(handle);
        if (runtime != null) {
            disposeAndRemove(handle, runtime);
        }
    }

    /**
     * Starts idempotent asynchronous shutdown and reclamation for Rust Drop.
     * This method never acknowledges an outstanding delivery and never waits.
     */
    public static void abandon(long handle) {
        ConnectorRuntime runtime = RUNTIMES.get(handle);
        if (runtime != null) {
            runtime.abandon(() -> disposeAndRemove(handle, runtime));
        }
    }

    /** Returns UTF-8 JSON diagnostics for startup and error reporting. */
    public static byte[] status(long handle) {
        return runtime(handle).status();
    }

    static long nextDeliveryToken() {
        return nextPositive(NEXT_DELIVERY_TOKEN, "delivery token");
    }

    static long nextPositive(AtomicLong sequence, String description) {
        while (true) {
            long value = sequence.get();
            if (value <= 0) {
                throw new IllegalStateException(description + " sequence is exhausted");
            }
            long next = value == Long.MAX_VALUE ? 0 : value + 1;
            if (sequence.compareAndSet(value, next)) {
                return value;
            }
        }
    }

    private static ConnectorRuntime runtime(long handle) {
        ConnectorRuntime runtime = RUNTIMES.get(handle);
        if (runtime == null) {
            throw new IllegalArgumentException("unknown Debezium connector handle");
        }
        return runtime;
    }

    private static void disposeAndRemove(long handle, ConnectorRuntime runtime) {
        synchronized (runtime) {
            if (!runtime.isDisposed()) {
                runtime.dispose();
            }
            RUNTIMES.remove(handle, runtime);
        }
    }
}
