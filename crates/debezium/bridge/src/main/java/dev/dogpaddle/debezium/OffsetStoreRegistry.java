package dev.dogpaddle.debezium;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetUtils;

/** Connects reflectively-created offset stores to their owning bridge runtime. */
final class OffsetStoreRegistry {
    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private OffsetStoreRegistry() {
    }

    static Entry register(String engineName, String connectorClass, Checkpoint checkpoint) {
        Checkpoint initial = checkpoint == null
                ? new Checkpoint(engineName, connectorClass, Map.of())
                : checkpoint;
        initial.requireBinding(engineName, connectorClass);
        Entry created = new Entry(initial);
        if (ENTRIES.putIfAbsent(engineName, created) != null) {
            throw new IllegalArgumentException(
                    "a Debezium engine named '" + engineName + "' already exists");
        }
        return created;
    }

    static Entry require(String engineName) {
        Entry entry = ENTRIES.get(engineName);
        if (entry == null) {
            throw new IllegalStateException(
                    "no DogPaddle offset registry entry for engine '" + engineName + "'");
        }
        return entry;
    }

    static void unregister(String engineName, Entry expected) {
        if (!ENTRIES.remove(engineName, expected)) {
            throw new IllegalStateException(
                    "DogPaddle offset registry ownership changed for engine '" + engineName + "'");
        }
    }

    static final class Entry {
        private Checkpoint current;
        private long version;
        private boolean attached;
        private boolean started;
        private PreparedCheckpoint expectedCommit;

        Entry(Checkpoint initial) {
            this.current = initial;
        }

        synchronized void attach() {
            if (attached) {
                throw new IllegalStateException(
                        "offset store is already attached to engine '" + current.engineName() + "'");
            }
            attached = true;
        }

        synchronized void start() {
            if (!attached || started) {
                throw new IllegalStateException(
                        "offset store cannot start in its current state for engine '"
                                + current.engineName() + "'");
            }
            started = true;
        }

        synchronized void stop() {
            started = false;
        }

        synchronized Map<ByteBuffer, ByteBuffer> get(Collection<ByteBuffer> keys) {
            requireStarted();
            Map<ByteBuffer, ByteBuffer> result = new LinkedHashMap<>();
            for (ByteBuffer requested : keys) {
                RawBytes key = RawBytes.from(Objects.requireNonNull(requested, "offset key"));
                RawBytes value = current.entries().get(key);
                result.put(key.buffer(), value == null ? null : value.buffer());
            }
            return result;
        }

        synchronized PreparedCheckpoint preview(Collection<SourceRecord> records) {
            requireStarted();
            if (expectedCommit != null) {
                throw new IllegalStateException("an offset commit is already armed");
            }
            Map<RawBytes, RawBytes> delta = OffsetPreviewer.preview(
                    current.engineName(), records);
            Checkpoint candidate = current.merge(delta);
            return new PreparedCheckpoint(
                    version,
                    Collections.unmodifiableMap(new TreeMap<>(delta)),
                    candidate,
                    CheckpointCodec.encode(candidate));
        }

        synchronized void arm(PreparedCheckpoint prepared) {
            requireStarted();
            if (expectedCommit != null) {
                throw new IllegalStateException("an offset commit is already armed");
            }
            if (prepared.baseVersion() != version) {
                throw new IllegalStateException("offset checkpoint became stale before ACK");
            }
            Checkpoint recalculated = current.merge(prepared.delta());
            if (!recalculated.equals(prepared.checkpoint())) {
                throw new IllegalStateException("prepared checkpoint does not match current offsets");
            }
            expectedCommit = prepared;
        }

        synchronized void applyActual(Map<ByteBuffer, ByteBuffer> values) {
            requireStarted();
            PreparedCheckpoint expected = expectedCommit;
            if (expected == null) {
                throw new IllegalStateException(
                        "Debezium attempted an offset write outside an acknowledged delivery");
            }
            Map<RawBytes, RawBytes> actual = rawMap(values);
            if (!actual.equals(expected.delta())) {
                throw new IllegalStateException(
                        "Debezium offset write differs from the pre-ACK checkpoint preview");
            }
            if (version != expected.baseVersion()) {
                throw new IllegalStateException("offset checkpoint changed during ACK");
            }
            Checkpoint candidate = current.merge(actual);
            if (!candidate.equals(expected.checkpoint())) {
                throw new IllegalStateException(
                        "Debezium offset write produced a different complete checkpoint");
            }
            current = candidate;
            version = Math.incrementExact(version);
            expectedCommit = null;
        }

        synchronized void disarm(PreparedCheckpoint prepared) {
            if (expectedCommit == prepared) {
                expectedCommit = null;
            }
        }

        synchronized void requireCommitted(PreparedCheckpoint prepared) {
            if (expectedCommit != null) {
                throw new IllegalStateException(
                        "Debezium did not commit the pre-ACK checkpoint");
            }
            long expectedVersion = Math.incrementExact(prepared.baseVersion());
            if (version != expectedVersion || !current.equals(prepared.checkpoint())) {
                throw new IllegalStateException(
                        "Debezium committed a different checkpoint than the pre-ACK preview");
            }
        }

        synchronized Checkpoint snapshot() {
            return current;
        }

        synchronized Set<Map<String, Object>> connectorPartitions(String connectorName) {
            requireStarted();
            JsonConverter keyConverter = new JsonConverter();
            keyConverter.configure(Map.of("schemas.enable", false), true);
            try {
                Map<String, Set<Map<String, Object>>> decoded = new HashMap<>();
                for (Map.Entry<RawBytes, RawBytes> offset : current.entries().entrySet()) {
                    OffsetUtils.processPartitionKey(
                            offset.getKey().bytes(),
                            offset.getValue().bytes(),
                            keyConverter,
                            decoded);
                }

                // Decoding creates an object graph independent from the raw checkpoint.
                // Copy its containers too, so no caller can mutate a later result.
                Set<Map<String, Object>> result = new LinkedHashSet<>();
                for (Map<String, Object> partition : decoded.getOrDefault(
                        connectorName, Set.of())) {
                    result.add(Collections.unmodifiableMap(new LinkedHashMap<>(partition)));
                }
                return Collections.unmodifiableSet(result);
            }
            finally {
                keyConverter.close();
            }
        }

        private void requireStarted() {
            if (!started) {
                throw new IllegalStateException(
                        "offset store is not started for engine '" + current.engineName() + "'");
            }
        }

        private static Map<RawBytes, RawBytes> rawMap(Map<ByteBuffer, ByteBuffer> values) {
            TreeMap<RawBytes, RawBytes> result = new TreeMap<>();
            for (Map.Entry<ByteBuffer, ByteBuffer> value : values.entrySet()) {
                RawBytes key = RawBytes.from(Objects.requireNonNull(value.getKey(), "offset key"));
                ByteBuffer bytes = value.getValue();
                result.put(key, bytes == null ? null : RawBytes.from(bytes));
            }
            return result;
        }
    }

    record PreparedCheckpoint(
            long baseVersion,
            Map<RawBytes, RawBytes> delta,
            Checkpoint checkpoint,
            byte[] encoded) {

        PreparedCheckpoint {
            encoded = encoded.clone();
        }

        @Override
        public byte[] encoded() {
            return encoded.clone();
        }
    }
}
