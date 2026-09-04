package dev.dogpaddle.experiments.debeziumd1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

final class ConnectorConfiguration {
    static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    static final String MEMORY_OFFSET_STORE =
            "org.apache.kafka.connect.storage.MemoryOffsetBackingStore";

    private ConnectorConfiguration() {
    }

    static Properties parse(byte[] json, ObjectMapper mapper) {
        final JsonNode root;
        try {
            root = mapper.readTree(json);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("connector configuration is not valid JSON", e);
        }
        if (!(root instanceof ObjectNode object)) {
            throw new IllegalArgumentException("connector configuration must be a JSON object");
        }

        Properties properties = new Properties();
        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode value = field.getValue();
            if (value == null || value.isNull() || value.isContainerNode()) {
                throw new IllegalArgumentException(
                        "connector property '" + field.getKey() + "' must be a scalar value");
            }
            properties.setProperty(field.getKey(), value.asText());
        }

        normalize(properties);
        return properties;
    }

    static void normalize(Properties properties) {
        String connectorClass = required(properties, "connector.class");
        required(properties, "name");

        // Preserve any standard Kafka Connect offset store selected by the caller.
        // Memory is only the disposable default for configurations that omit one.
        properties.putIfAbsent("offset.storage", MEMORY_OFFSET_STORE);

        if (POSTGRES_CONNECTOR.equals(connectorClass)) {
            String flushMode = properties.getProperty("lsn.flush.mode", "connector");
            if (!"connector".equals(flushMode)) {
                throw new IllegalArgumentException(
                        "PostgreSQL D1 requires lsn.flush.mode=connector; refusing unsafe mode '"
                                + flushMode + "'");
            }
            properties.setProperty("lsn.flush.mode", "connector");

            String legacyFlush = properties.getProperty("flush.lsn.source");
            if (legacyFlush != null && !Boolean.parseBoolean(legacyFlush)) {
                throw new IllegalArgumentException(
                        "PostgreSQL D1 requires flush.lsn.source=true when the legacy property is present");
            }
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing connector property '" + key + "'");
        }
        return value;
    }
}
