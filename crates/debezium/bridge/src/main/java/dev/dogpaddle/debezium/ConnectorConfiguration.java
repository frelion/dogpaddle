package dev.dogpaddle.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/** Parses connector properties and owns all engine-critical configuration. */
final class ConnectorConfiguration {
    private static final String OFFSET_STORE = DogPaddleOffsetBackingStore.class.getName();

    private final Properties properties;
    private final String engineName;
    private final String connectorClass;

    private ConnectorConfiguration(
            Properties properties, String engineName, String connectorClass) {
        this.properties = properties;
        this.engineName = engineName;
        this.connectorClass = connectorClass;
    }

    static ConnectorConfiguration parse(byte[] json, ObjectMapper mapper) {
        final JsonNode root;
        try {
            root = mapper.readTree(json);
        }
        catch (IOException error) {
            throw new IllegalArgumentException("connector configuration is not valid JSON", error);
        }
        if (!(root instanceof ObjectNode object)) {
            throw new IllegalArgumentException("connector configuration must be a JSON object");
        }

        Properties properties = new Properties();
        Iterator<Map.Entry<String, JsonNode>> fields = object.properties().iterator();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();
            JsonNode value = field.getValue();
            if (key.isBlank()) {
                throw new IllegalArgumentException("connector property name must not be blank");
            }
            if (isReserved(key)) {
                throw new IllegalArgumentException(
                        "connector property '" + key + "' is owned by DogPaddle");
            }
            if (value == null || value.isNull() || value.isContainerNode()) {
                throw new IllegalArgumentException(
                        "connector property '" + key + "' must be a scalar value");
            }
            properties.setProperty(key, value.asText());
        }

        String engineName = required(properties, "name");
        String connectorClass = required(properties, "connector.class");
        properties.setProperty("offset.storage", OFFSET_STORE);
        properties.setProperty("tasks.max", "1");
        properties.setProperty("record.processing.threads", "1");
        properties.setProperty("record.processing.order", "ORDERED");
        return new ConnectorConfiguration(properties, engineName, connectorClass);
    }

    Properties properties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    String engineName() {
        return engineName;
    }

    String connectorClass() {
        return connectorClass;
    }

    private static boolean isReserved(String key) {
        return key.startsWith("offset.")
                || key.equals("tasks.max")
                || key.equals("transforms")
                || key.startsWith("transforms.")
                || key.equals("predicates")
                || key.startsWith("predicates.")
                || key.startsWith("record.processing.")
                || key.equals("key.converter")
                || key.startsWith("key.converter.")
                || key.equals("value.converter")
                || key.startsWith("value.converter.")
                || key.equals("header.converter")
                || key.startsWith("header.converter.")
                || key.startsWith("dogpaddle.");
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing connector property '" + key + "'");
        }
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "connector property '" + key + "' is not canonical UTF-8");
        }
        if (encoded.length > CheckpointCodec.MAX_BINDING_BYTES) {
            throw new IllegalArgumentException(
                    "connector property '" + key + "' exceeds "
                            + CheckpointCodec.MAX_BINDING_BYTES + " UTF-8 bytes");
        }
        return value;
    }
}
