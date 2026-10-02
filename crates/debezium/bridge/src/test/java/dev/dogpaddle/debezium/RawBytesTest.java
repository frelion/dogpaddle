package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

final class RawBytesTest {
    @Test
    void remaining_bytes_are_owned_without_changing_the_callers_buffer() {
        byte[] contents = {9, 0, (byte) 0xff, 3, 8};
        ByteBuffer heap = ByteBuffer.wrap(contents.clone());
        ByteBuffer direct = ByteBuffer.allocateDirect(contents.length);
        direct.put(contents).clear();
        ByteBuffer parent = ByteBuffer.wrap(new byte[] {7, 9, 0, (byte) 0xff, 3, 8, 6});
        parent.position(1).limit(6);
        for (ByteBuffer buffer : List.of(
                heap, direct, ByteBuffer.wrap(contents.clone()).asReadOnlyBuffer(), parent.slice())) {
            buffer.position(1).limit(4).mark();
            RawBytes value = RawBytes.from(buffer);
            assertArrayEquals(new byte[] {0, (byte) 0xff, 3}, value.bytes());
            assertEquals(1, buffer.position());
            assertEquals(4, buffer.limit());
            buffer.reset();
            assertEquals(1, buffer.position());
            if (!buffer.isReadOnly()) {
                buffer.put(1, (byte) 42);
                assertArrayEquals(new byte[] {0, (byte) 0xff, 3}, value.bytes());
            }
        }
    }

    @Test
    void byte_array_construction_and_exports_remain_isolated() {
        byte[] original = {0, (byte) 0xff};
        RawBytes value = new RawBytes(original);
        RawBytes equal = RawBytes.from(ByteBuffer.wrap(original));
        original[0] = 42;
        byte[] exported = value.bytes();
        exported[1] = 42;
        ByteBuffer exportedBuffer = value.buffer();
        exportedBuffer.put(0, (byte) 42);
        assertArrayEquals(new byte[] {0, (byte) 0xff}, value.bytes());
        assertEquals(equal, value);
        assertEquals(equal.hashCode(), value.hashCode());
        assertEquals(0, value.compareTo(equal));
        assertEquals(1, Integer.signum(value.compareTo(new RawBytes(new byte[] {0, 127}))));
        assertEquals(-1, Integer.signum(value.compareTo(new RawBytes(new byte[] {1}))));
    }

    @Test
    void empty_and_null_buffers_keep_their_existing_meanings() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {42});
        buffer.position(1);
        assertArrayEquals(new byte[0], RawBytes.from(buffer).bytes());
        assertEquals(1, buffer.position());
        assertThrows(NullPointerException.class, () -> RawBytes.from(null));
    }
}
