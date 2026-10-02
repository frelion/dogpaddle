package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

class CheckpointCodecTest {
    private static final byte[] GOLDEN = HexFormat.of().parseHex(
            "4450444243503031000100000008656e67696e652d610000000b636f6e6e6563746f722e41"
                    + "000000020000000200ff000000020102000000017a00000001031ef7d5c2");
    private static final byte[] NON_CANONICAL_TOMBSTONE = HexFormat.of().parseHex(
            "4450444243503031000100000008656e67696e652d610000000b636f6e6e6563746f722e41"
                    + "000000020000000200ff000000020102000000017affffffff50f1cac2");

    @Test
    void checkpoint_has_canonical_golden_bytes_and_round_trips() {
        TreeMap<RawBytes, RawBytes> entries = new TreeMap<>();
        entries.put(new RawBytes(new byte[] {0, (byte) 0xff}), new RawBytes(new byte[] {1, 2}));
        entries.put(new RawBytes(new byte[] {'z'}), new RawBytes(new byte[] {3}));
        Checkpoint checkpoint = new Checkpoint("engine-a", "connector.A", entries);

        byte[] encoded = CheckpointCodec.encode(checkpoint);

        assertArrayEquals(GOLDEN, encoded);
        assertEquals(checkpoint, CheckpointCodec.decode(encoded));
        assertArrayEquals(encoded, CheckpointCodec.encode(CheckpointCodec.decode(encoded)));
    }

    @Test
    void checkpoint_rejects_corruption_and_wrong_binding() {
        byte[] corrupted = GOLDEN.clone();
        corrupted[20] ^= 1;

        IllegalArgumentException checksum = assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointCodec.decode(corrupted));
        assertEquals("checkpoint checksum mismatch", checksum.getMessage());

        Checkpoint decoded = CheckpointCodec.decode(GOLDEN);
        assertThrows(
                IllegalArgumentException.class,
                () -> decoded.requireBinding("another-engine", "connector.A"));
    }

    @Test
    void merging_a_delta_replaces_values_and_removes_tombstones() {
        RawBytes retained = new RawBytes(new byte[] {1});
        RawBytes removed = new RawBytes(new byte[] {2});
        Checkpoint checkpoint = new Checkpoint(
                "engine-a",
                "connector.A",
                Map.of(retained, new RawBytes(new byte[] {3}),
                        removed, new RawBytes(new byte[] {4})));
        TreeMap<RawBytes, RawBytes> delta = new TreeMap<>();
        delta.put(retained, new RawBytes(new byte[] {5}));
        delta.put(removed, null);

        Checkpoint merged = checkpoint.merge(delta);

        assertArrayEquals(new byte[] {5}, merged.entries().get(retained).bytes());
        assertEquals(1, merged.entries().size());
    }

    @Test
    void checkpoint_rejects_an_empty_raw_offset_key() {
        TreeMap<RawBytes, RawBytes> entries = new TreeMap<>();
        entries.put(new RawBytes(new byte[0]), new RawBytes(new byte[] {1}));
        Checkpoint checkpoint = new Checkpoint("engine-a", "connector.A", entries);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointCodec.encode(checkpoint));

        assertEquals("checkpoint offset key must not be empty", error.getMessage());
    }

    @Test
    void complete_checkpoint_rejects_a_tombstone_value() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointCodec.decode(NON_CANONICAL_TOMBSTONE));
    }

    @Test
    void checkpoint_encode_rejects_an_oversized_entry_component() {
        TreeMap<RawBytes, RawBytes> entries = new TreeMap<>();
        entries.put(
                new RawBytes(new byte[32 * 1024 * 1024 + 1]),
                new RawBytes(new byte[] {1}));
        Checkpoint checkpoint = new Checkpoint("engine-a", "connector.A", entries);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointCodec.encode(checkpoint));

        assertEquals(
                "checkpoint offset entry exceeds 33554432 bytes",
                error.getMessage());
    }

    @Test
    void checkpoint_rejects_an_oversized_binding_before_encoding() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new Checkpoint(
                        "x".repeat(1024 * 1024 + 1),
                        "connector.A",
                        Map.of()));

        assertEquals(
                "engine name exceeds 1048576 UTF-8 bytes",
                error.getMessage());
    }

    @Test
    void checkpoint_encode_accepts_the_exact_limit_and_rejects_one_more_byte() {
        int maximumBytes = 64 * 1024 * 1024;
        RawBytes firstValue = new RawBytes(new byte[32 * 1024 * 1024]);
        for (int extra = 0; extra <= 1; extra++) {
            // Two one-byte bindings and keys occupy 46 bytes including the checksum.
            Checkpoint checkpoint = new Checkpoint("a", "b", Map.of(
                    new RawBytes(new byte[] {1}), firstValue,
                    new RawBytes(new byte[] {2}),
                    new RawBytes(new byte[maximumBytes - 46 - firstValue.size() + extra])));
            if (extra == 0) {
                assertEquals(maximumBytes, CheckpointCodec.encode(checkpoint).length);
            }
            else {
                IllegalArgumentException error = assertThrows(
                        IllegalArgumentException.class,
                        () -> CheckpointCodec.encode(checkpoint));
                assertEquals("checkpoint exceeds 67108864 bytes", error.getMessage());
            }
        }
    }

    @Test
    void checkpoint_checksum_crosses_the_initial_buffer_boundary_without_padding() {
        for (int bodyBytes : new int[] {8191, 8192, 8193}) {
            // The body has 33 framing bytes for one entry and one-byte bindings/key.
            Checkpoint checkpoint = new Checkpoint("a", "b", Map.of(
                    new RawBytes(new byte[] {1}), new RawBytes(new byte[bodyBytes - 33])));
            byte[] encoded = CheckpointCodec.encode(checkpoint);
            assertEquals(bodyBytes + Integer.BYTES, encoded.length);
            CRC32 checksum = new CRC32();
            checksum.update(encoded, 0, bodyBytes);
            assertEquals(checksum.getValue(), Integer.toUnsignedLong(
                    ByteBuffer.wrap(encoded, bodyBytes, Integer.BYTES).getInt()));
            assertEquals(checkpoint, CheckpointCodec.decode(encoded));
        }
    }

    @Test
    void checkpoint_binding_rejects_an_unpaired_utf16_surrogate() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new Checkpoint("engine-\ud800", "connector.A", Map.of()));

        assertEquals("engine name is not canonical UTF-8", error.getMessage());
    }

    @Test
    void shortest_non_blank_binding_round_trips() {
        Checkpoint checkpoint = new Checkpoint("a", "b", Map.of());

        byte[] encoded = CheckpointCodec.encode(checkpoint);

        assertEquals(28, encoded.length);
        assertEquals(checkpoint, CheckpointCodec.decode(encoded));
    }

    @Test
    void validated_binding_keeps_exact_utf8_bytes_at_the_protocol_limit() {
        for (String binding : new String[] {
                "x".repeat(1024 * 1024), "é".repeat(512 * 1024), "  名字\u0000  "}) {
            byte[] expected = binding.getBytes(StandardCharsets.UTF_8);
            Checkpoint checkpoint = new Checkpoint(binding, binding, Map.of());
            byte[] encoded = CheckpointCodec.encode(checkpoint);
            ByteBuffer frame = ByteBuffer.wrap(encoded);
            frame.position(CheckpointCodec.MAGIC.length + Short.BYTES);
            for (int field = 0; field < 2; field++) {
                assertEquals(expected.length, frame.getInt());
                byte[] actual = new byte[expected.length];
                frame.get(actual);
                assertArrayEquals(expected, actual);
            }
            assertEquals(checkpoint, CheckpointCodec.decode(encoded));
        }
        for (boolean engine : new boolean[] {true, false}) {
            String tooLarge = "é".repeat(512 * 1024) + "x";
            String description = engine ? "engine name" : "connector class";
            assertEquals(description + " exceeds 1048576 UTF-8 bytes", assertThrows(
                    IllegalArgumentException.class,
                    () -> new Checkpoint(engine ? tooLarge : "e", engine ? "c" : tooLarge,
                            Map.of())).getMessage());
            assertEquals(description + " is not canonical UTF-8", assertThrows(
                    IllegalArgumentException.class,
                    () -> new Checkpoint(engine ? "\ud800" : "e", engine ? "c" : "\ud800",
                            Map.of())).getMessage());
        }
    }

    @Test
    void incoming_binding_bytes_are_validated_even_with_a_valid_checksum() {
        byte[] encoded = CheckpointCodec.encode(new Checkpoint("é", "é", Map.of()));
        for (int badByte : new int[] {14, 15, 20, 21}) {
            byte[] malformed = encoded.clone();
            malformed[badByte] = (byte) 0xff;
            CRC32 checksum = new CRC32();
            checksum.update(malformed, 0, malformed.length - Integer.BYTES);
            ByteBuffer.wrap(malformed, malformed.length - Integer.BYTES, Integer.BYTES)
                    .putInt((int) checksum.getValue());
            assertEquals("checkpoint binding is not canonical UTF-8", assertThrows(
                    IllegalArgumentException.class,
                    () -> CheckpointCodec.decode(malformed)).getMessage());
        }
    }
}
