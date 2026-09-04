package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ConnectorConfigurationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void configuration_forces_the_bridge_owned_engine_settings() {
        ConnectorConfiguration configuration = parse(
                "{\"name\":\"source-a\",\"connector.class\":\"example.Connector\","
                        + "\"database.hostname\":\"db\"}");

        Properties properties = configuration.properties();
        assertEquals("source-a", configuration.engineName());
        assertEquals("example.Connector", configuration.connectorClass());
        assertEquals(
                DogPaddleOffsetBackingStore.class.getName(),
                properties.getProperty("offset.storage"));
        assertEquals("1", properties.getProperty("tasks.max"));
        assertEquals("1", properties.getProperty("record.processing.threads"));
        assertEquals("ORDERED", properties.getProperty("record.processing.order"));
        assertEquals("db", properties.getProperty("database.hostname"));
    }

    @Test
    void checkpoint_bindings_enforce_the_utf8_byte_limit_during_configuration() {
        String accepted = "x".repeat(CheckpointCodec.MAX_BINDING_BYTES);
        ConnectorConfiguration configuration = parse(
                "{\"name\":\"" + accepted + "\",\"connector.class\":\"example.Connector\"}");
        assertEquals(accepted, configuration.engineName());

        String rejected = accepted + "x";
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> parse(
                        "{\"name\":\"source-a\",\"connector.class\":\""
                                + rejected + "\"}"));
        assertEquals(
                "connector property 'connector.class' exceeds 1048576 UTF-8 bytes",
                error.getMessage());
    }

    @ParameterizedTest
    @MethodSource("reservedProperties")
    void configuration_rejects_engine_owned_properties(String property) {
        String json = "{\"name\":\"source-a\","
                + "\"connector.class\":\"example.Connector\",\""
                + property + "\":\"unsafe\"}";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> parse(json));

        assertEquals(
                "connector property '" + property + "' is owned by DogPaddle",
                error.getMessage());
    }

    static Stream<String> reservedProperties() {
        return Stream.of(
                "offset.storage",
                "offset.flush.interval.ms",
                "offset.commit.policy",
                "tasks.max",
                "transforms",
                "transforms.mask.type",
                "predicates",
                "predicates.is-tombstone.type",
                "record.processing.order",
                "key.converter",
                "dogpaddle.private");
    }

    private ConnectorConfiguration parse(String json) {
        return ConnectorConfiguration.parse(json.getBytes(StandardCharsets.UTF_8), mapper);
    }
}
