package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.debezium.embedded.EmbeddedWorkerConfig;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.jupiter.api.Test;

class OffsetPreviewerTest {
    @Test
    void reflectively_constructed_store_attaches_through_the_public_worker_config() {
        String engineName = "configured-store";
        OffsetStoreRegistry.Entry entry = OffsetStoreRegistry.register(
                engineName, "example.Connector", null);
        DogPaddleOffsetBackingStore store = new DogPaddleOffsetBackingStore();
        try {
            store.configure(new EmbeddedWorkerConfig(new HashMap<>(Map.of(
                    "name", engineName,
                    "connector.class", "example.Connector",
                    "offset.storage", DogPaddleOffsetBackingStore.class.getName()))));
            store.start();
            assertEquals(java.util.Set.of(), store.connectorPartitions(engineName));
        }
        finally {
            store.stop();
            OffsetStoreRegistry.unregister(engineName, entry);
        }
    }

    @Test
    void multi_partition_preview_exactly_matches_the_actual_store_write() throws Exception {
        Checkpoint initial = new Checkpoint("engine-a", "example.Connector", Map.of());
        OffsetStoreRegistry.Entry entry = new OffsetStoreRegistry.Entry(initial);
        DogPaddleOffsetBackingStore actualStore = new DogPaddleOffsetBackingStore(entry);
        actualStore.start();
        List<SourceRecord> records = List.of(
                record("a", 1L),
                record("b", 7L),
                record("a", 2L));

        OffsetStoreRegistry.PreparedCheckpoint prepared = entry.preview(records);

        assertEquals(2, prepared.delta().size());
        assertEquals(2, prepared.checkpoint().entries().size());
        assertNotEquals(initial, prepared.checkpoint());

        entry.arm(prepared);
        flushWithPublicWriter(actualStore, records);
        entry.requireCommitted(prepared);

        assertEquals(prepared.checkpoint(), entry.snapshot());
        assertEquals(
                prepared.checkpoint(),
                CheckpointCodec.decode(prepared.encoded()));

        var expectedPartitions = java.util.Set.of(
                Map.<String, Object>of("partition", "a"),
                Map.<String, Object>of("partition", "b"));
        var firstPartitions = actualStore.connectorPartitions("engine-a");
        var secondPartitions = actualStore.connectorPartitions("engine-a");
        assertEquals(expectedPartitions, firstPartitions);
        assertEquals(expectedPartitions, secondPartitions);
        assertNotSame(
                firstPartitions.iterator().next(),
                secondPartitions.iterator().next());
        assertEquals(java.util.Set.of(), actualStore.connectorPartitions("another-engine"));
    }

    @Test
    void actual_store_write_fails_closed_when_it_differs_from_preview() {
        OffsetStoreRegistry.Entry entry = new OffsetStoreRegistry.Entry(
                new Checkpoint("engine-a", "example.Connector", Map.of()));
        entry.attach();
        entry.start();
        OffsetStoreRegistry.PreparedCheckpoint prepared =
                entry.preview(List.of(record("a", 1L)));
        entry.arm(prepared);
        Map<ByteBuffer, ByteBuffer> wrong = asBuffers(prepared.delta());
        ByteBuffer key = wrong.keySet().iterator().next();
        wrong.put(key, ByteBuffer.wrap(new byte[] {9}));

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> entry.applyActual(wrong));

        assertEquals(
                "Debezium offset write differs from the pre-ACK checkpoint preview",
                error.getMessage());
        assertEquals(0, entry.snapshot().entries().size());
        entry.disarm(prepared);
    }

    @Test
    void checkpoint_restores_through_the_real_kafka_connect_offset_reader()
            throws Exception {
        OffsetStoreRegistry.Entry producing = new OffsetStoreRegistry.Entry(
                new Checkpoint("engine-a", "example.Connector", Map.of()));
        DogPaddleOffsetBackingStore producingStore = new DogPaddleOffsetBackingStore(producing);
        producingStore.start();
        List<SourceRecord> records = List.of(record("a", 1L), record("a", 2L));
        OffsetStoreRegistry.PreparedCheckpoint prepared = producing.preview(records);
        producing.arm(prepared);
        flushWithPublicWriter(producingStore, records);
        producing.requireCommitted(prepared);

        Checkpoint restoredCheckpoint = CheckpointCodec.decode(prepared.encoded());
        OffsetStoreRegistry.Entry restored = new OffsetStoreRegistry.Entry(restoredCheckpoint);
        DogPaddleOffsetBackingStore restoredStore = new DogPaddleOffsetBackingStore(restored);
        restoredStore.start();
        JsonConverter keyConverter = converter(true);
        JsonConverter valueConverter = converter(false);
        try (OffsetStorageReaderImpl reader = new OffsetStorageReaderImpl(
                restoredStore, "engine-a", keyConverter, valueConverter)) {
            Map<String, Object> offset = reader.offset(Map.of("partition", "a"));
            assertEquals(2L, offset.get("position"));
        }
        finally {
            keyConverter.close();
            valueConverter.close();
        }
    }

    @Test
    void missing_actual_commit_cannot_be_reported_as_an_ack_success() {
        OffsetStoreRegistry.Entry entry = new OffsetStoreRegistry.Entry(
                new Checkpoint("engine-a", "example.Connector", Map.of()));
        DogPaddleOffsetBackingStore store = new DogPaddleOffsetBackingStore(entry);
        store.start();
        OffsetStoreRegistry.PreparedCheckpoint prepared =
                entry.preview(List.of(record("a", 1L)));
        entry.arm(prepared);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> entry.requireCommitted(prepared));

        assertEquals("Debezium did not commit the pre-ACK checkpoint", error.getMessage());
        entry.disarm(prepared);
    }

    private static SourceRecord record(String partition, long position) {
        return new SourceRecord(
                Map.of("partition", partition),
                Map.of("position", position),
                "topic",
                0,
                Schema.STRING_SCHEMA,
                partition,
                Schema.INT64_SCHEMA,
                position,
                123L);
    }

    private static Map<ByteBuffer, ByteBuffer> asBuffers(
            Map<RawBytes, RawBytes> values) {
        Map<ByteBuffer, ByteBuffer> buffers = new LinkedHashMap<>();
        for (Map.Entry<RawBytes, RawBytes> value : values.entrySet()) {
            buffers.put(
                    value.getKey().buffer(),
                    value.getValue() == null ? null : value.getValue().buffer());
        }
        return buffers;
    }

    private static void flushWithPublicWriter(
            DogPaddleOffsetBackingStore store, List<SourceRecord> records) throws Exception {
        JsonConverter keyConverter = converter(true);
        JsonConverter valueConverter = converter(false);
        try {
            OffsetStorageWriter writer = new OffsetStorageWriter(
                    store, "engine-a", keyConverter, valueConverter);
            for (SourceRecord record : records) {
                writer.offset(record.sourcePartition(), record.sourceOffset());
            }
            if (!writer.beginFlush()) {
                throw new AssertionError("actual writer produced no offset delta");
            }
            writer.doFlush((error, ignored) -> {
                if (error != null) {
                    throw new AssertionError(error);
                }
            }).get();
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
}
