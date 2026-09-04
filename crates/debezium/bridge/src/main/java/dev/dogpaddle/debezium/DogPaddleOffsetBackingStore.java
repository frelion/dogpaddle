package dev.dogpaddle.debezium;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;

/**
 * Public Kafka Connect SPI adapter backed by a bridge-owned in-memory registry.
 *
 * <p>The store is deliberately not durable. Its initial and accepted states
 * come from opaque checkpoint bytes owned by Rust.</p>
 */
public final class DogPaddleOffsetBackingStore implements OffsetBackingStore {
    private OffsetStoreRegistry.Entry entry;

    public DogPaddleOffsetBackingStore() {
    }

    DogPaddleOffsetBackingStore(OffsetStoreRegistry.Entry entry) {
        entry.attach();
        this.entry = entry;
    }

    @Override
    public void configure(WorkerConfig config) {
        Object name = config.originals().get("name");
        if (!(name instanceof String engineName) || engineName.isBlank()) {
            throw new IllegalArgumentException(
                    "DogPaddle offset store requires the Debezium engine name");
        }
        OffsetStoreRegistry.Entry configured = OffsetStoreRegistry.require(engineName);
        configured.attach();
        entry = configured;
    }

    @Override
    public void start() {
        requireConfigured().start();
    }

    @Override
    public void stop() {
        OffsetStoreRegistry.Entry configured = entry;
        if (configured != null) {
            configured.stop();
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        try {
            return CompletableFuture.completedFuture(requireConfigured().get(keys));
        }
        catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public Future<Void> set(
            Map<ByteBuffer, ByteBuffer> values,
            Callback<Void> callback) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            requireConfigured().applyActual(values);
        }
        catch (Throwable error) {
            if (callback != null) {
                callback.onCompletion(error, null);
            }
            result.completeExceptionally(error);
            return result;
        }
        try {
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            result.complete(null);
        }
        catch (Throwable callbackFailure) {
            result.completeExceptionally(callbackFailure);
        }
        return result;
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return requireConfigured().connectorPartitions(connectorName);
    }

    private OffsetStoreRegistry.Entry requireConfigured() {
        if (entry == null) {
            throw new IllegalStateException("DogPaddle offset store is not configured");
        }
        return entry;
    }
}
