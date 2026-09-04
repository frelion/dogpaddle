package dev.dogpaddle.debezium;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class ReclamationTestTask extends SourceTask {
    private ReclamationTestConnector.Control control;

    @Override
    public String version() {
        return "test";
    }

    @Override
    public void start(Map<String, String> properties) {
        control = ReclamationTestConnector.control(
                properties.get(ReclamationTestConnector.CONTROL_NAME));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        control.pollStarted();
        Thread.sleep(Long.MAX_VALUE);
        return List.of();
    }

    @Override
    public void stop() {
        control.taskStopStarted();
        control.awaitTaskStopRelease();
    }
}
