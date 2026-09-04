package dev.dogpaddle.experiments.debeziumd1;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The deliberately small, connector-neutral JNI surface used by the D1 host.
 *
 * <p>All payloads crossing JNI are UTF-8 JSON bytes. The bridge never invokes
 * native code: Rust owns the command loop and pulls deliveries from Java.</p>
 */
public final class D1Bridge {
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);
    private static final AtomicLong NEXT_DELIVERY_TOKEN = new AtomicLong(1);
    private static final Map<Long, ConnectorRuntime> RUNTIMES = new ConcurrentHashMap<>();
    private static final String JVM_ID = UUID.randomUUID().toString();
    private static final long JAVA_PROCESS_ID = ProcessHandle.current().pid();

    private D1Bridge() {
    }

    /** Creates an engine instance from a JSON object containing connector properties. */
    public static long create(byte[] configurationJson) {
        ConnectorRuntime runtime = ConnectorRuntime.create(
                configurationJson,
                D1Bridge::nextDeliveryToken,
                JVM_ID,
                JAVA_PROCESS_ID);
        long handle = NEXT_HANDLE.getAndIncrement();
        RUNTIMES.put(handle, runtime);
        return handle;
    }

    /** Starts the engine thread. A handle is single-use and can be started once. */
    public static void start(long handle) {
        runtime(handle).start();
    }

    /**
     * Returns one delivery as UTF-8 JSON, or {@code null} when the timeout
     * expires. An outstanding delivery is returned byte-for-byte until ACK.
     */
    public static byte[] poll(long handle, long timeoutMillis, int maxBytes) {
        return runtime(handle).poll(timeoutMillis, maxBytes);
    }

    /**
     * Acknowledges the exact outstanding token. This method returns only after
     * Debezium's handler thread has marked every record and finished the batch.
     */
    public static void ack(long handle, long token) {
        runtime(handle).ack(token);
    }

    /** Stops the engine and waits up to the supplied timeout for its thread. */
    public static void stop(long handle, long timeoutMillis) {
        runtime(handle).stop(timeoutMillis);
    }

    /** Returns a UTF-8 JSON status object. */
    public static byte[] status(long handle) {
        return runtime(handle).status();
    }

    static long nextDeliveryToken() {
        return nextDeliveryToken(NEXT_DELIVERY_TOKEN);
    }

    static long nextDeliveryToken(AtomicLong sequence) {
        while (true) {
            long token = sequence.get();
            if (token <= 0) {
                throw new IllegalStateException("D1 delivery token sequence is exhausted");
            }
            long next = token == Long.MAX_VALUE ? 0 : token + 1;
            if (sequence.compareAndSet(token, next)) {
                return token;
            }
        }
    }

    private static ConnectorRuntime runtime(long handle) {
        ConnectorRuntime runtime = RUNTIMES.get(handle);
        if (runtime == null) {
            throw new IllegalArgumentException("unknown D1 bridge handle: " + handle);
        }
        return runtime;
    }
}
