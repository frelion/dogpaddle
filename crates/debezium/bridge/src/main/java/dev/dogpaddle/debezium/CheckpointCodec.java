package dev.dogpaddle.debezium;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/** Canonical codec for the complete, connector-bound offset checkpoint. */
final class CheckpointCodec {
    static final byte[] MAGIC = "DPDBCP01".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 1;

    private static final int CHECKSUM_BYTES = Integer.BYTES;
    private static final int MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024;
    static final int MAX_BINDING_BYTES = 1024 * 1024;
    private static final int MAX_ENTRY_BYTES = 32 * 1024 * 1024;
    private static final int MAX_ENTRIES = 1_000_000;

    private CheckpointCodec() {
    }

    static byte[] encode(Checkpoint checkpoint) {
        try {
            BoundedBytes bytes = new BoundedBytes(
                    MAX_CHECKPOINT_BYTES - CHECKSUM_BYTES);
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(VERSION);
            writeRequiredUtf8(output, checkpoint.engineName());
            writeRequiredUtf8(output, checkpoint.connectorClass());
            if (checkpoint.entries().size() > MAX_ENTRIES) {
                throw new IllegalArgumentException(
                        "checkpoint has too many offset entries");
            }
            output.writeInt(checkpoint.entries().size());
            for (Map.Entry<RawBytes, RawBytes> entry : checkpoint.entries().entrySet()) {
                if (entry.getKey().size() == 0) {
                    throw new IllegalArgumentException("checkpoint offset key must not be empty");
                }
                if (entry.getKey().size() > MAX_ENTRY_BYTES
                        || (entry.getValue() != null
                                && entry.getValue().size() > MAX_ENTRY_BYTES)) {
                    throw new IllegalArgumentException(
                            "checkpoint offset entry exceeds " + MAX_ENTRY_BYTES + " bytes");
                }
                writeRequiredBytes(output, entry.getKey().bytes());
                writeRequiredBytes(output, entry.getValue().bytes());
            }
            output.flush();
            byte[] body = bytes.toByteArray();
            CRC32 checksum = new CRC32();
            checksum.update(body);
            return ByteBuffer.allocate(body.length + CHECKSUM_BYTES)
                    .put(body)
                    .putInt((int) checksum.getValue())
                    .array();
        }
        catch (IOException error) {
            throw new IllegalStateException("cannot encode checkpoint", error);
        }
    }

    static Checkpoint decode(byte[] encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("checkpoint bytes must not be null");
        }
        if (encoded.length > MAX_CHECKPOINT_BYTES) {
            throw new IllegalArgumentException(
                    "checkpoint exceeds " + MAX_CHECKPOINT_BYTES + " bytes");
        }
        int minimum = MAGIC.length
                + Short.BYTES
                + Integer.BYTES * 3
                + 2
                + CHECKSUM_BYTES;
        if (encoded.length < minimum) {
            throw new IllegalArgumentException("checkpoint is truncated");
        }

        int bodyLength = encoded.length - CHECKSUM_BYTES;
        long expectedChecksum = Integer.toUnsignedLong(
                ByteBuffer.wrap(encoded, bodyLength, CHECKSUM_BYTES).getInt());
        CRC32 checksum = new CRC32();
        checksum.update(encoded, 0, bodyLength);
        if (checksum.getValue() != expectedChecksum) {
            throw new IllegalArgumentException("checkpoint checksum mismatch");
        }

        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(encoded, 0, bodyLength));
            byte[] magic = input.readNBytes(MAGIC.length);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("checkpoint magic mismatch");
            }
            int version = input.readUnsignedShort();
            if (version != VERSION) {
                throw new IllegalArgumentException(
                        "unsupported checkpoint version " + version);
            }
            String engineName = readRequiredUtf8(input);
            String connectorClass = readRequiredUtf8(input);
            int entryCount = readCount(input, MAX_ENTRIES, "checkpoint entry count");
            TreeMap<RawBytes, RawBytes> entries = new TreeMap<>();
            RawBytes previous = null;
            for (int index = 0; index < entryCount; index++) {
                RawBytes key = new RawBytes(readRequiredBytes(input, MAX_ENTRY_BYTES));
                if (key.size() == 0) {
                    throw new IllegalArgumentException("checkpoint offset key must not be empty");
                }
                if (previous != null && previous.compareTo(key) >= 0) {
                    throw new IllegalArgumentException(
                            "checkpoint keys are not in canonical order");
                }
                RawBytes value = new RawBytes(readRequiredBytes(input, MAX_ENTRY_BYTES));
                entries.put(key, value);
                previous = key;
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException("checkpoint has trailing bytes");
            }
            return new Checkpoint(engineName, connectorClass, entries);
        }
        catch (EOFException error) {
            throw new IllegalArgumentException("checkpoint is truncated", error);
        }
        catch (IOException error) {
            throw new IllegalArgumentException("cannot decode checkpoint", error);
        }
    }

    private static void writeRequiredUtf8(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "checkpoint binding is not canonical UTF-8");
        }
        if (encoded.length == 0 || encoded.length > MAX_BINDING_BYTES) {
            throw new IllegalArgumentException("checkpoint binding has invalid length");
        }
        writeRequiredBytes(output, encoded);
    }

    private static String readRequiredUtf8(DataInputStream input) throws IOException {
        byte[] encoded = readRequiredBytes(input, MAX_BINDING_BYTES);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!Arrays.equals(encoded, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("checkpoint binding is not canonical UTF-8");
        }
        return value;
    }

    private static void writeRequiredBytes(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readRequiredBytes(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximum || length > input.available()) {
            throw new IllegalArgumentException("invalid checkpoint component length " + length);
        }
        return input.readNBytes(length);
    }

    private static int readCount(DataInputStream input, int maximum, String description)
            throws IOException {
        int value = input.readInt();
        if (value < 0 || value > maximum) {
            throw new IllegalArgumentException("invalid " + description + " " + value);
        }
        return value;
    }

    private static final class BoundedBytes extends ByteArrayOutputStream {
        private final int maximumBytes;

        BoundedBytes(int maximumBytes) {
            super(Math.min(maximumBytes, 8192));
            this.maximumBytes = maximumBytes;
        }

        @Override
        public synchronized void write(int value) {
            requireCapacity(1);
            super.write(value);
        }

        @Override
        public synchronized void write(byte[] bytes, int offset, int length) {
            requireCapacity(length);
            super.write(bytes, offset, length);
        }

        private void requireCapacity(int additional) {
            if (additional < 0 || count > maximumBytes - additional) {
                throw new IllegalArgumentException(
                        "checkpoint exceeds " + MAX_CHECKPOINT_BYTES + " bytes");
            }
        }
    }
}
