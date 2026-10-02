package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Checks armed offset ownership across reentrant caller maps. */
final class ArmedCheckpointReuseTest {
    private static final String CHANGED =
            "armed checkpoint changed while reading actual offsets";

    @Test
    void reentrant_other_commit_cannot_be_overwritten_by_outer_actual() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint original = prepared(entry, 1, 11);
        OffsetStoreRegistry.PreparedCheckpoint other = prepared(entry, 2, 22);
        entry.arm(original);
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> entry.applyActual(reentrant(buffers(original.delta()), () -> {
                    entry.disarm(original);
                    entry.arm(other);
                    entry.applyActual(buffers(other.delta()));
                })));
        assertEquals(CHANGED, error.getMessage());
        assertEquals(other.checkpoint(), entry.snapshot());
        entry.requireCommitted(other);
    }

    @Test
    void changed_arm_is_checked_before_actual_delta_mismatch() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint original = prepared(entry, 1, 11);
        entry.arm(original);
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> entry.applyActual(reentrant(Map.of(), () -> entry.disarm(original))));
        assertEquals(CHANGED, error.getMessage());
        assertEquals(Map.of(), entry.snapshot().entries());
    }

    @Test
    void different_prepared_remains_armed_after_outer_rejection() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint original = prepared(entry, 1, 11);
        OffsetStoreRegistry.PreparedCheckpoint other = prepared(entry, 2, 22);
        entry.arm(original);
        assertEquals(CHANGED, assertThrows(IllegalStateException.class,
                () -> entry.applyActual(reentrant(buffers(original.delta()), () -> {
                    entry.disarm(original);
                    entry.arm(other);
                }))).getMessage());
        assertEquals("Debezium did not commit the pre-ACK checkpoint",
                assertThrows(IllegalStateException.class,
                        () -> entry.requireCommitted(other)).getMessage());
        entry.applyActual(buffers(other.delta()));
        entry.requireCommitted(other);
    }

    @Test
    void same_prepared_aba_rechecks_candidate_against_new_current() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint original = prepared(entry, 1, 11);
        OffsetStoreRegistry.PreparedCheckpoint other = prepared(entry, 1, 22);
        entry.arm(original);
        entry.applyActual(reentrant(buffers(original.delta()), () -> {
            entry.disarm(original);
            entry.arm(other);
            entry.applyActual(buffers(other.delta()));
            entry.arm(original);
        }));
        entry.requireCommitted(original);
        assertEquals(original.checkpoint(), entry.snapshot());
    }

    @Test
    void incompatible_same_prepared_aba_fails_in_retained_arm_check() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint original = prepared(entry, 1, 11);
        OffsetStoreRegistry.PreparedCheckpoint other = prepared(entry, 2, 22);
        entry.arm(original);
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> entry.applyActual(reentrant(buffers(original.delta()), () -> {
                    entry.disarm(original);
                    entry.arm(other);
                    entry.applyActual(buffers(other.delta()));
                    entry.arm(original);
                })));
        assertEquals("prepared checkpoint does not match current offsets", error.getMessage());
        assertEquals(other.checkpoint(), entry.snapshot());
        entry.requireCommitted(other);
    }

    @Test
    void caller_delta_mutation_after_arm_cannot_change_frozen_preparation() {
        OffsetStoreRegistry.Entry entry = started();
        Map<RawBytes, RawBytes> delta = new HashMap<>();
        delta.put(raw(1), raw(11));
        OffsetStoreRegistry.PreparedCheckpoint prepared =
                new OffsetStoreRegistry.PreparedCheckpoint(delta, entry.snapshot().merge(delta));
        entry.arm(prepared);
        delta.clear();
        delta.put(raw(2), raw(22));
        entry.applyActual(buffers(prepared.delta()));
        entry.requireCommitted(prepared);
        assertEquals(Map.of(raw(1), raw(11)), entry.snapshot().entries());
    }

    @Test
    void conversion_failure_precedes_identity_recheck_and_does_not_publish() {
        OffsetStoreRegistry.Entry entry = started();
        OffsetStoreRegistry.PreparedCheckpoint prepared = prepared(entry, 1, 11);
        entry.arm(prepared);
        Map<ByteBuffer, ByteBuffer> malformed = new HashMap<>();
        malformed.put(null, ByteBuffer.wrap(new byte[] {11}));
        NullPointerException error = assertThrows(NullPointerException.class,
                () -> entry.applyActual(reentrant(malformed, () -> entry.disarm(prepared))));
        assertEquals("offset key", error.getMessage());
        assertEquals(Map.of(), entry.snapshot().entries());
    }

    private static OffsetStoreRegistry.Entry started() {
        OffsetStoreRegistry.Entry entry = new OffsetStoreRegistry.Entry(
                new Checkpoint("engine", "connector", Map.of()));
        entry.attach();
        entry.start();
        return entry;
    }

    private static OffsetStoreRegistry.PreparedCheckpoint prepared(
            OffsetStoreRegistry.Entry entry, int key, int value) {
        Map<RawBytes, RawBytes> delta = Map.of(raw(key), raw(value));
        return new OffsetStoreRegistry.PreparedCheckpoint(delta, entry.snapshot().merge(delta));
    }

    private static RawBytes raw(int value) {
        return new RawBytes(new byte[] {(byte) value});
    }

    private static Map<ByteBuffer, ByteBuffer> buffers(Map<RawBytes, RawBytes> delta) {
        Map<ByteBuffer, ByteBuffer> output = new HashMap<>();
        delta.forEach((key, value) -> output.put(key.buffer(), value == null ? null : value.buffer()));
        return output;
    }

    private static Map<ByteBuffer, ByteBuffer> reentrant(
            Map<ByteBuffer, ByteBuffer> values, Runnable callback) {
        return new AbstractMap<>() {
            @Override
            public Set<Map.Entry<ByteBuffer, ByteBuffer>> entrySet() {
                callback.run();
                return values.entrySet();
            }
        };
    }
}
