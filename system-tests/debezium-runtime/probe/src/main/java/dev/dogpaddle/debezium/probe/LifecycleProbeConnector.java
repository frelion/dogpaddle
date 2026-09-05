package dev.dogpaddle.debezium.probe;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/** A deterministic connector packaged only for the native bundle lifecycle probe. */
public final class LifecycleProbeConnector extends SourceConnector {
    @Override
    public void start(Map<String, String> properties) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ProbeTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maximumTasks) {
        if (maximumTasks < 1) {
            throw new IllegalArgumentException("lifecycle probe requires one task");
        }
        return List.of(Map.of());
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return "probe";
    }

    /** Public because Kafka Connect constructs task classes reflectively. */
    public static final class ProbeTask extends SourceTask {
        private static final Map<String, String> SOURCE_PARTITION =
                Map.of("source", "dogpaddle-lifecycle-probe");

        private boolean emitted;
        private long nextPosition;

        @Override
        public String version() {
            return "probe";
        }

        @Override
        public void start(Map<String, String> properties) {
            Map<String, Object> restored = context.offsetStorageReader()
                    .offset(SOURCE_PARTITION);
            if (restored == null) {
                nextPosition = 1L;
                return;
            }
            Object position = restored.get("position");
            if (!(position instanceof Number number) || number.longValue() < 1L) {
                throw new IllegalStateException("lifecycle probe restored an invalid offset");
            }
            nextPosition = Math.incrementExact(number.longValue());
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (emitted) {
                Thread.sleep(50L);
                return List.of();
            }
            emitted = true;
            long position = nextPosition;
            SourceRecord record = new SourceRecord(
                    SOURCE_PARTITION,
                    Map.of("position", position),
                    "dogpaddle-lifecycle-probe",
                    7,
                    Schema.STRING_SCHEMA,
                    "probe-key-" + position,
                    Schema.STRING_SCHEMA,
                    "probe-value-" + position,
                    Math.addExact(1_700_000_000_000L, position));
            record.headers().addString("probe-header-a", "header-a-" + position);
            record.headers().addString("probe-header-b", "header-b-" + position);
            return List.of(record);
        }

        @Override
        public void stop() {
        }
    }
}
