package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class DeliveryCodecTest {
    private static final byte[] GOLDEN = HexFormat.of().parseHex(
            "44504442445630310002000000334450444243503031000100000008656e67696e652d61"
                    + "000000116578616d706c652e436f6e6e6563746f720000000033b0f22100000002000000"
                    + "05746f7069630000003f7b22736368656d61223a7b2274797065223a22737472696e6722"
                    + "2c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a22666972737422"
                    + "7d00000005746f706963000000407b22736368656d61223a7b2274797065223a22737472"
                    + "696e67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a227365"
                    + "636f6e64227de6f872b3");

    @Test
    void delivery_contains_checkpoint_and_owned_connect_json_in_source_order() {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        byte[] checkpointBytes = CheckpointCodec.encode(checkpoint);
        SourceRecord first = record("first", 1, 10L);
        first.headers().addInt("attempt", 3);
        SourceRecord second = record("second", 2, 11L);

        byte[] encoded;
        try (DeliveryCodec codec = new DeliveryCodec()) {
            encoded = codec.encode(
                    checkpointBytes, List.of(first, second), 64 * 1024);
        }
        assertArrayEquals(GOLDEN, encoded);
    }

    @Test
    void minimum_delivery_is_one_null_record_plus_the_exact_checkpoint() {
        byte[] checkpoint = CheckpointCodec.encode(new Checkpoint("e", "c", Map.of()));
        SourceRecord record = new SourceRecord(Map.of(), Map.of(), null, null, null);
        assertEquals(28, checkpoint.length);
        try (DeliveryCodec codec = new DeliveryCodec()) {
            byte[] encoded = codec.encode(checkpoint, List.of(record), 58);
            assertEquals(58, encoded.length);
            assertEquals(DeliveryCodec.MINIMUM_BYTES_EXCLUDING_CHECKPOINT,
                    encoded.length - checkpoint.length);
            assertEquals(DeliveryCodec.MINIMUM_MAXIMUM_BYTES, encoded.length);
            assertThrows(IllegalArgumentException.class,
                    () -> codec.encode(checkpoint, List.of(record), 57));
        }
    }

    @Test
    void unexported_key_and_headers_are_not_converted_or_validated() {
        byte[] checkpoint = CheckpointCodec.encode(new Checkpoint("e", "c", Map.of()));
        SourceRecord record = new SourceRecord(
                Map.of("partition", 1), Map.of("position", 1L), "topic", 7,
                Schema.INT32_SCHEMA, "not an integer", Schema.STRING_SCHEMA, "value", 10L);
        record.headers().addString("unused-\ud800", "value");
        SourceRecord plain = new SourceRecord(
                Map.of("partition", 1), Map.of("position", 1L), "topic",
                Schema.STRING_SCHEMA, "value");
        try (DeliveryCodec codec = new DeliveryCodec()) {
            assertArrayEquals(codec.encode(checkpoint, List.of(plain), 1024),
                    codec.encode(checkpoint, List.of(record), 1024));
        }
    }

    @Test
    void exported_value_still_requires_a_valid_connect_schema_value_pair() {
        byte[] checkpoint = CheckpointCodec.encode(new Checkpoint("e", "c", Map.of()));
        SourceRecord record = new SourceRecord(
                Map.of(), Map.of(), "topic", Schema.INT32_SCHEMA, "not an integer");
        try (DeliveryCodec codec = new DeliveryCodec()) {
            assertThrows(org.apache.kafka.connect.errors.DataException.class,
                    () -> codec.encode(checkpoint, List.of(record), 1024));
        }
    }

    @Test
    void delivery_checksum_covers_the_body_at_the_exact_size_limit_after_buffer_growth() {
        byte[] checkpoint = CheckpointCodec.encode(
                new Checkpoint("engine-a", "example.Connector", Map.of()));
        SourceRecord record = record("x".repeat(8192), 1, 10L);

        try (DeliveryCodec codec = new DeliveryCodec()) {
            byte[] encoded = codec.encode(checkpoint, List.of(record), 64 * 1024);
            assertTrue(encoded.length > 8192);

            CRC32 checksum = new CRC32();
            checksum.update(encoded, 0, encoded.length - Integer.BYTES);
            long actual = Integer.toUnsignedLong(
                    ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES, Integer.BYTES)
                            .getInt());
            assertEquals(checksum.getValue(), actual);
            assertArrayEquals(encoded, codec.encode(checkpoint, List.of(record), encoded.length));

            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> codec.encode(checkpoint, List.of(record), encoded.length - 1));
            assertTrue(DeliveryCodec.isTooLarge(error));
        }
    }

    @Test
    void delivery_size_limit_fails_closed() {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        SourceRecord record = record("x".repeat(1024), 1, 10L);

        try (DeliveryCodec codec = new DeliveryCodec()) {
            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> codec.encode(
                            CheckpointCodec.encode(checkpoint),
                            List.of(record),
                            128));
            assertTrue(error.getMessage().contains("exceeds maximum"));
            assertTrue(DeliveryCodec.isTooLarge(error));
        }
    }

    @Test
    void failure_kind_does_not_classify_unrelated_connector_errors() {
        assertFalse(DeliveryCodec.isTooLarge(new IllegalStateException("failed")));
    }

    @Test
    void delivery_rejects_text_that_cannot_round_trip_through_utf8() {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        SourceRecord record = new SourceRecord(
                Map.of("partition", 1),
                Map.of("position", 1L),
                "topic-\ud800",
                Schema.STRING_SCHEMA,
                "value");

        try (DeliveryCodec codec = new DeliveryCodec()) {
            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> codec.encode(
                            CheckpointCodec.encode(checkpoint),
                            List.of(record),
                            1024));
            assertEquals("SourceRecord text is not canonical UTF-8", error.getMessage());
        }
    }

    private static SourceRecord record(String value, int partition, long timestamp) {
        return new SourceRecord(
                Map.of("partition", partition),
                Map.of("position", timestamp),
                "topic",
                partition,
                Schema.STRING_SCHEMA,
                value,
                Schema.STRING_SCHEMA,
                value,
                timestamp);
    }
}
