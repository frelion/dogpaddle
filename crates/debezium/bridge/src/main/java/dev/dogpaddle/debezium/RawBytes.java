package dev.dogpaddle.debezium;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Immutable bytes with unsigned lexicographic ordering. */
final class RawBytes implements Comparable<RawBytes> {
    private final byte[] bytes;

    RawBytes(byte[] bytes) {
        this.bytes = bytes.clone();
    }

    static RawBytes from(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new RawBytes(bytes);
    }

    byte[] bytes() {
        return bytes.clone();
    }

    ByteBuffer buffer() {
        // Kafka Connect 4.3's OffsetStorageReaderImpl accesses value.array().
        // A fresh heap buffer is still isolated from our immutable bytes.
        return ByteBuffer.wrap(bytes());
    }

    int size() {
        return bytes.length;
    }

    @Override
    public int compareTo(RawBytes other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RawBytes value && Arrays.equals(bytes, value.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
