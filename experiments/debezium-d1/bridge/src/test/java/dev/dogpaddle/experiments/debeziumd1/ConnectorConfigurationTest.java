package dev.dogpaddle.experiments.debeziumd1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ConnectorConfigurationTest {
    @Test
    void preserves_an_explicit_file_offset_store() {
        Properties properties = postgresProperties();
        properties.setProperty(
                "offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/d1-offsets.dat");

        ConnectorConfiguration.normalize(properties);

        assertEquals(
                "org.apache.kafka.connect.storage.FileOffsetBackingStore",
                properties.getProperty("offset.storage"));
        assertEquals(
                "/tmp/d1-offsets.dat",
                properties.getProperty("offset.storage.file.filename"));
    }

    @Test
    void defaults_only_an_omitted_offset_store_to_memory() {
        Properties properties = postgresProperties();

        ConnectorConfiguration.normalize(properties);

        assertEquals(
                ConnectorConfiguration.MEMORY_OFFSET_STORE,
                properties.getProperty("offset.storage"));
        assertEquals("connector", properties.getProperty("lsn.flush.mode"));
    }

    @Test
    void rejects_postgres_driver_side_lsn_flushing() {
        Properties properties = postgresProperties();
        properties.setProperty("lsn.flush.mode", "connector_and_driver");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ConnectorConfiguration.normalize(properties));

        assertEquals(
                "PostgreSQL D1 requires lsn.flush.mode=connector; refusing unsafe mode 'connector_and_driver'",
                error.getMessage());
    }

    private static Properties postgresProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", "test");
        properties.setProperty("connector.class", ConnectorConfiguration.POSTGRES_CONNECTOR);
        return properties;
    }
}
