package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class DeliveryCodecTest {
    private static final byte[] GOLDEN = HexFormat.of().parseHex(
            "44504442445630310001000000334450444243503031000100000008656e67696e652d61000000116578616d"
                    + "706c652e436f6e6e6563746f720000000033b0f2210000000200000005746f70696301000000010100000000"
                    + "0000000a0000003f7b22736368656d61223a7b2274797065223a22737472696e67222c226f7074696f6e616c"
                    + "223a66616c73657d2c227061796c6f6164223a226669727374227d0000003f7b22736368656d61223a7b2274"
                    + "797065223a22737472696e67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a2266"
                    + "69727374227d0000000100000007617474656d7074000000387b22736368656d61223a7b2274797065223a22"
                    + "696e743332222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a337d00000005746f70"
                    + "6963010000000201000000000000000b000000407b22736368656d61223a7b2274797065223a22737472696e"
                    + "67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a227365636f6e64227d00000040"
                    + "7b22736368656d61223a7b2274797065223a22737472696e67222c226f7074696f6e616c223a66616c73657d"
                    + "2c227061796c6f6164223a227365636f6e64227d00000000f6e0afee");

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
