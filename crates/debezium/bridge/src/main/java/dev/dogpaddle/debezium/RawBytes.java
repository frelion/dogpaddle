package dev.dogpaddle.debezium;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Immutable bytes with unsigned lexicographic ordering. */
final class RawBytes implements Comparable<RawBytes> {
    private final byte[] bytes;

    RawBytes(byte[] bytes) {
        this.bytes = bytes.clone();
    }

    private RawBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        this.bytes = new byte[copy.remaining()];
        copy.get(this.bytes);
    }

    static RawBytes from(ByteBuffer buffer) {
        return new RawBytes(buffer);
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
