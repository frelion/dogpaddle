package dev.dogpaddle.debezium;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/** A controllable connector used only by bridge lifecycle tests. */
public final class ReclamationTestConnector extends SourceConnector {
    static final String CONTROL_NAME = "test.control.name";

    private static final Map<String, Control> CONTROLS = new ConcurrentHashMap<>();

    private String controlName;

    @Override
    public void start(Map<String, String> properties) {
        controlName = properties.get("name");
        Control control = control(controlName);
        if (control.failStartup) {
            throw new IllegalStateException("deliberate connector startup failure");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ReclamationTestTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maximumTasks) {
        return List.of(Map.of(CONTROL_NAME, controlName));
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
        return "test";
    }

    static Control install(String name, boolean failStartup, boolean blockTaskStop) {
        Control control = new Control(failStartup, blockTaskStop);
        if (CONTROLS.putIfAbsent(name, control) != null) {
            throw new IllegalStateException("test control already exists for " + name);
        }
        return control;
    }

    static void uninstall(String name, Control control) {
        if (!CONTROLS.remove(name, control)) {
            throw new IllegalStateException("test control ownership changed for " + name);
        }
    }

    static Control control(String name) {
        Control control = CONTROLS.get(name);
        if (control == null) {
            throw new IllegalStateException("missing test control for " + name);
        }
        return control;
    }

    static final class Control {
        private final boolean failStartup;
        private final CountDownLatch pollStarted = new CountDownLatch(1);
        private final CountDownLatch taskStopStarted = new CountDownLatch(1);
        private final CountDownLatch releaseTaskStop;

        private Control(boolean failStartup, boolean blockTaskStop) {
            this.failStartup = failStartup;
            releaseTaskStop = new CountDownLatch(blockTaskStop ? 1 : 0);
        }

        boolean awaitPollStarted(long timeoutMillis) throws InterruptedException {
            return pollStarted.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        boolean awaitTaskStopStarted(long timeoutMillis) throws InterruptedException {
            return taskStopStarted.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        void pollStarted() {
            pollStarted.countDown();
        }

        void taskStopStarted() {
            taskStopStarted.countDown();
        }

        void awaitTaskStopRelease() {
            boolean interrupted = false;
            while (true) {
                try {
                    releaseTaskStop.await();
                    break;
                }
                catch (InterruptedException error) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        void releaseTaskStop() {
            releaseTaskStop.countDown();
        }
    }
}
