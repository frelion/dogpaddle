package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class DeliveryCodecTest {
    private static final byte[] GOLDEN = HexFormat.of().parseHex(
            "44504442445630310001000000000000002a000000334450444243503031000100000008656e6769"
                    + "6e652d61000000116578616d706c652e436f6e6e6563746f720000000033b0f22100000002000000"
                    + "05746f706963010000000101000000000000000a0000003f7b22736368656d61223a7b2274797065"
                    + "223a22737472696e67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a22"
                    + "6669727374227d0000003f7b22736368656d61223a7b2274797065223a22737472696e67222c226f"
                    + "7074696f6e616c223a66616c73657d2c227061796c6f6164223a226669727374227d000000010000"
                    + "0007617474656d7074000000387b22736368656d61223a7b2274797065223a22696e743332222c22"
                    + "6f7074696f6e616c223a66616c73657d2c227061796c6f6164223a337d00000005746f7069630100"
                    + "00000201000000000000000b000000407b22736368656d61223a7b2274797065223a22737472696e"
                    + "67222c226f7074696f6e616c223a66616c73657d2c227061796c6f6164223a227365636f6e64227d"
                    + "000000407b22736368656d61223a7b2274797065223a22737472696e67222c226f7074696f6e616c"
                    + "223a66616c73657d2c227061796c6f6164223a227365636f6e64227d00000000903264ff");

    @Test
    void delivery_contains_checkpoint_and_owned_connect_json_in_source_order()
            throws Exception {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        byte[] checkpointBytes = CheckpointCodec.encode(checkpoint);
        SourceRecord first = record("first", 1, 10L);
        first.headers().addInt("attempt", 3);
        SourceRecord second = record("second", 2, 11L);

        byte[] encoded;
        try (DeliveryCodec codec = new DeliveryCodec()) {
            encoded = codec.encode(
                    42, checkpointBytes, List.of(first, second), 64 * 1024);
        }
        assertArrayEquals(GOLDEN, encoded);

        int bodyLength = encoded.length - Integer.BYTES;
        CRC32 checksum = new CRC32();
        checksum.update(encoded, 0, bodyLength);
        assertEquals(
                checksum.getValue(),
                Integer.toUnsignedLong(ByteBuffer.wrap(encoded, bodyLength, 4).getInt()));

        DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(encoded, 0, bodyLength));
        assertArrayEquals(DeliveryCodec.MAGIC, input.readNBytes(DeliveryCodec.MAGIC.length));
        assertEquals(DeliveryCodec.VERSION, input.readUnsignedShort());
        assertEquals(42, input.readLong());
        assertArrayEquals(checkpointBytes, readRequired(input));
        assertEquals(2, input.readInt());

        assertEquals("topic", readNullableUtf8(input));
        assertEquals(1, input.readUnsignedByte());
        assertEquals(1, input.readInt());
        assertEquals(1, input.readUnsignedByte());
        assertEquals(10L, input.readLong());
        assertTrue(readNullableUtf8(input).contains("first"));
        assertTrue(readNullableUtf8(input).contains("first"));
        assertEquals(1, input.readInt());
        assertEquals("attempt", new String(readRequired(input), StandardCharsets.UTF_8));
        assertTrue(readNullableUtf8(input).contains("3"));

        assertEquals("topic", readNullableUtf8(input));
        assertEquals(1, input.readUnsignedByte());
        assertEquals(2, input.readInt());
        assertEquals(1, input.readUnsignedByte());
        assertEquals(11L, input.readLong());
        assertTrue(readNullableUtf8(input).contains("second"));
        assertTrue(readNullableUtf8(input).contains("second"));
        assertEquals(0, input.readInt());
        assertEquals(0, input.available());
    }

    @Test
    void delivery_size_limit_fails_closed() {
        Checkpoint checkpoint = new Checkpoint("engine-a", "example.Connector", Map.of());
        SourceRecord record = record("x".repeat(1024), 1, 10L);

        try (DeliveryCodec codec = new DeliveryCodec()) {
            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> codec.encode(
                            1,
                            CheckpointCodec.encode(checkpoint),
                            List.of(record),
                            128));
            assertTrue(error.getMessage().contains("exceeds maximum"));
            assertEquals("delivery_too_large", DeliveryCodec.failureKind(error));
        }
    }

    @Test
    void failure_kind_does_not_classify_unrelated_connector_errors() {
        assertNull(DeliveryCodec.failureKind(new IllegalStateException("failed")));
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
                            1,
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

    private static byte[] readRequired(DataInputStream input) throws Exception {
        int length = input.readInt();
        return input.readNBytes(length);
    }

    private static String readNullableUtf8(DataInputStream input) throws Exception {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }
}
