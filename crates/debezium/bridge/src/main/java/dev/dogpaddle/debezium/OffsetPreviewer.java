package dev.dogpaddle.debezium;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;

/** Uses Kafka Connect's public writer to reproduce Debezium's raw offset delta. */
final class OffsetPreviewer {
    private OffsetPreviewer() {
    }

    static Map<RawBytes, RawBytes> preview(
            String engineName, Collection<SourceRecord> records) {
        CaptureStore capture = new CaptureStore();
        JsonConverter keyConverter = converter(true);
        JsonConverter valueConverter = converter(false);
        try {
            OffsetStorageWriter writer = new OffsetStorageWriter(
                    capture, engineName, keyConverter, valueConverter);
            for (SourceRecord record : records) {
                if (record.sourcePartition() == null || record.sourceOffset() == null) {
                    throw new IllegalArgumentException(
                            "Debezium emitted a SourceRecord without partition or offset");
                }
                writer.offset(record.sourcePartition(), record.sourceOffset());
            }
            if (!writer.beginFlush()) {
                throw new IllegalStateException(
                        "Kafka Connect produced no offset delta for a non-empty delivery");
            }
            writer.doFlush((error, ignored) -> {
                if (error != null) {
                    capture.failure = error;
                }
            }).get();
            if (capture.failure != null) {
                throw new IllegalStateException("cannot preview offset checkpoint", capture.failure);
            }
            return capture.captured();
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("offset preview was interrupted", error);
        }
        catch (java.util.concurrent.ExecutionException error) {
            throw new IllegalStateException("cannot preview offset checkpoint", error.getCause());
        }
        finally {
            keyConverter.close();
            valueConverter.close();
        }
    }

    private static JsonConverter converter(boolean isKey) {
        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", false), isKey);
        return converter;
    }

    private static final class CaptureStore implements OffsetBackingStore {
        private Map<RawBytes, RawBytes> delta;
        private Throwable failure;

        @Override
        public void configure(WorkerConfig config) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public Future<Void> set(
                Map<ByteBuffer, ByteBuffer> values,
                Callback<Void> callback) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            try {
                if (delta != null) {
                    throw new IllegalStateException("offset preview flushed more than once");
                }
                TreeMap<RawBytes, RawBytes> captured = new TreeMap<>();
                for (Map.Entry<ByteBuffer, ByteBuffer> value : values.entrySet()) {
                    RawBytes key = RawBytes.from(value.getKey());
                    captured.put(
                            key,
                            value.getValue() == null ? null : RawBytes.from(value.getValue()));
                }
                delta = captured;
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
            return Set.of();
        }

        Map<RawBytes, RawBytes> captured() {
            if (delta == null) {
                throw new IllegalStateException("offset preview did not flush");
            }
            return delta;
        }
    }
}
