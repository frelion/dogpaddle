package dev.dogpaddle.debezium;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

/** Encodes one complete, Rust-owned delivery with an exact size ceiling. */
final class DeliveryCodec implements AutoCloseable {
    static final byte[] MAGIC = "DPDBDV01".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 1;
    static final int MINIMUM_MAXIMUM_BYTES = 76;
    static final int MINIMUM_BYTES_EXCLUDING_CHECKPOINT = 48;

    private static final int CHECKSUM_BYTES = Integer.BYTES;

    private final JsonConverter keyConverter;
    private final JsonConverter valueConverter;

    DeliveryCodec() {
        keyConverter = converter(true);
        valueConverter = converter(false);
    }

    byte[] encode(
            long token,
            byte[] checkpoint,
            List<SourceRecord> records,
            int maximumBytes) {
        if (token <= 0) {
            throw new IllegalArgumentException("delivery token must be positive");
        }
        if (checkpoint == null) {
            throw new IllegalArgumentException("delivery checkpoint must not be null");
        }
        if (records.isEmpty()) {
            throw new IllegalArgumentException("a delivery must contain at least one SourceRecord");
        }
        if (maximumBytes < MINIMUM_MAXIMUM_BYTES) {
            throw new IllegalArgumentException("maximum delivery bytes is too small");
        }

        try {
            BoundedBytes bytes = new BoundedBytes(maximumBytes);
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(VERSION);
            output.writeLong(token);
            writeRequiredBytes(output, checkpoint);
            output.writeInt(records.size());
            for (SourceRecord record : records) {
                encodeRecord(output, record);
            }
            output.flush();
            if (bytes.size() > maximumBytes - CHECKSUM_BYTES) {
                throw tooLarge(maximumBytes);
            }
            byte[] body = bytes.toByteArray();
            CRC32 checksum = new CRC32();
            checksum.update(body);
            output.writeInt((int) checksum.getValue());
            output.flush();
            return bytes.toByteArray();
        }
        catch (DeliveryTooLarge error) {
            throw new IllegalStateException(
                    "delivery exceeds maximum of " + maximumBytes + " bytes", error);
        }
        catch (IOException error) {
            throw new IllegalStateException("cannot encode Debezium delivery", error);
        }
    }

    @Override
    public void close() {
        keyConverter.close();
        valueConverter.close();
    }

    private void encodeRecord(DataOutputStream output, SourceRecord record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("Debezium emitted a null SourceRecord");
        }
        writeNullableUtf8(output, record.topic());

        Integer partition = record.kafkaPartition();
        output.writeByte(partition == null ? 0 : 1);
        if (partition != null) {
            output.writeInt(partition);
        }

        Long timestamp = record.timestamp();
        output.writeByte(timestamp == null ? 0 : 1);
        if (timestamp != null) {
            output.writeLong(timestamp);
        }

        writeNullableBytes(
                output,
                keyConverter.fromConnectData(
                        record.topic(), record.keySchema(), record.key()));
        writeNullableBytes(
                output,
                valueConverter.fromConnectData(
                        record.topic(), record.valueSchema(), record.value()));

        int headerCount = 0;
        for (Header ignored : record.headers()) {
            headerCount = Math.incrementExact(headerCount);
        }
        output.writeInt(headerCount);
        for (Header header : record.headers()) {
            if (header.key() == null) {
                throw new IllegalArgumentException("SourceRecord header key must not be null");
            }
            writeRequiredUtf8(output, header.key());
            writeNullableBytes(
                    output,
                    valueConverter.fromConnectHeader(
                            record.topic(), header.key(), header.schema(), header.value()));
        }
    }

    private static JsonConverter converter(boolean isKey) {
        JsonConverter converter = new JsonConverter();
        converter.configure(
                Map.of("schemas.enable", true, "replace.null.with.default", false), isKey);
        return converter;
    }

    private static void writeNullableUtf8(DataOutputStream output, String value)
            throws IOException {
        writeNullableBytes(
                output, value == null ? null : canonicalUtf8(value));
    }

    private static void writeRequiredUtf8(DataOutputStream output, String value)
            throws IOException {
        writeRequiredBytes(output, canonicalUtf8(value));
    }

    private static byte[] canonicalUtf8(String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "SourceRecord text is not canonical UTF-8");
        }
        return encoded;
    }

    private static void writeRequiredBytes(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static void writeNullableBytes(DataOutputStream output, byte[] value)
            throws IOException {
        if (value == null) {
            output.writeInt(-1);
        }
        else {
            writeRequiredBytes(output, value);
        }
    }

    private static DeliveryTooLarge tooLarge(int maximumBytes) {
        return new DeliveryTooLarge(maximumBytes);
    }

    static String failureKind(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof DeliveryTooLarge) {
                return "delivery_too_large";
            }
            current = current.getCause();
        }
        return null;
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
                throw tooLarge(maximumBytes);
            }
        }
    }

    private static final class DeliveryTooLarge extends RuntimeException {
        DeliveryTooLarge(int maximumBytes) {
            super("delivery exceeds maximum of " + maximumBytes + " bytes");
        }
    }
}
