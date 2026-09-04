package dev.dogpaddle.debezium;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** A connector-bound, complete snapshot of Kafka Connect's raw offset store. */
final class Checkpoint {
    private final String engineName;
    private final String connectorClass;
    private final Map<RawBytes, RawBytes> entries;

    Checkpoint(String engineName, String connectorClass, Map<RawBytes, RawBytes> entries) {
        this.engineName = requireNonBlank(engineName, "engine name");
        this.connectorClass = requireNonBlank(connectorClass, "connector class");
        for (Map.Entry<RawBytes, RawBytes> entry : entries.entrySet()) {
            Objects.requireNonNull(entry.getKey(), "checkpoint offset key");
            Objects.requireNonNull(entry.getValue(), "checkpoint offset value");
        }
        this.entries = Collections.unmodifiableMap(new TreeMap<>(entries));
    }

    String engineName() {
        return engineName;
    }

    String connectorClass() {
        return connectorClass;
    }

    Map<RawBytes, RawBytes> entries() {
        return entries;
    }

    Checkpoint merge(Map<RawBytes, RawBytes> delta) {
        TreeMap<RawBytes, RawBytes> merged = new TreeMap<>(entries);
        for (Map.Entry<RawBytes, RawBytes> entry : delta.entrySet()) {
            if (entry.getValue() == null) {
                merged.remove(entry.getKey());
            }
            else {
                merged.put(entry.getKey(), entry.getValue());
            }
        }
        return new Checkpoint(engineName, connectorClass, merged);
    }

    void requireBinding(String expectedName, String expectedClass) {
        if (!engineName.equals(expectedName) || !connectorClass.equals(expectedClass)) {
            throw new IllegalArgumentException(
                    "checkpoint belongs to engine '" + engineName + "' and connector '"
                            + connectorClass + "', not engine '" + expectedName
                            + "' and connector '" + expectedClass + "'");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Checkpoint value)) {
            return false;
        }
        return engineName.equals(value.engineName)
                && connectorClass.equals(value.connectorClass)
                && entries.equals(value.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(engineName, connectorClass, entries);
    }

    private static String requireNonBlank(String value, String description) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(description + " must not be blank");
        }
        byte[] encoded = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, java.nio.charset.StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(description + " is not canonical UTF-8");
        }
        if (encoded.length > CheckpointCodec.MAX_BINDING_BYTES) {
            throw new IllegalArgumentException(
                    description + " exceeds " + CheckpointCodec.MAX_BINDING_BYTES + " UTF-8 bytes");
        }
        return value;
    }
}
