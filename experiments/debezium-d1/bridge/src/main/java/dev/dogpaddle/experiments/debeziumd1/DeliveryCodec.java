package dev.dogpaddle.experiments.debeziumd1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

final class DeliveryCodec {
    private final ObjectMapper mapper;
    private final JsonConverter keyConverter;
    private final JsonConverter valueConverter;

    DeliveryCodec(ObjectMapper mapper) {
        this.mapper = mapper;
        this.keyConverter = converter(true);
        this.valueConverter = converter(false);
    }

    byte[] encode(long token, List<SourceRecord> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("a delivery must contain at least one SourceRecord");
        }

        ObjectNode root = mapper.createObjectNode();
        root.put("protocol", 1);
        root.put("kind", "delivery");
        root.put("state", "running");
        root.put("outstanding", true);
        root.put("token", token);
        root.put("event_count", records.size());
        root.set("partition", canonicalMap(records.get(0).sourcePartition()));
        root.set("offset", canonicalMap(records.get(records.size() - 1).sourceOffset()));

        ArrayNode events = root.putArray("events");
        for (SourceRecord record : records) {
            events.add(event(record));
        }

        try {
            return mapper.writeValueAsBytes(root);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode Debezium delivery", e);
        }
    }

    private ObjectNode event(SourceRecord record) {
        ObjectNode event = mapper.createObjectNode();
        putNullableText(event, "topic", record.topic());
        putNullableInteger(event, "kafka_partition", record.kafkaPartition());
        putNullableLong(event, "timestamp", record.timestamp());
        event.set("partition", canonicalMap(record.sourcePartition()));
        event.set("offset", canonicalMap(record.sourceOffset()));
        event.set("key", connectJson(keyConverter, record.topic(), record.keySchema(), record.key()));
        event.set("value", connectJson(valueConverter, record.topic(), record.valueSchema(), record.value()));

        ArrayNode headers = event.putArray("headers");
        for (Header header : record.headers()) {
            ObjectNode encoded = headers.addObject();
            encoded.put("key", header.key());
            encoded.set(
                    "value",
                    connectJson(valueConverter, record.topic(), header.schema(), header.value()));
        }
        return event;
    }

    private JsonNode connectJson(
            JsonConverter converter, String topic, Schema schema, Object value) {
        byte[] json = converter.fromConnectData(topic, schema, value);
        if (json == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        try {
            return mapper.readTree(json);
        }
        catch (IOException e) {
            throw new IllegalStateException("Kafka Connect JsonConverter returned invalid JSON", e);
        }
    }

    private ObjectNode canonicalMap(Map<String, ?> values) {
        ObjectNode object = mapper.createObjectNode();
        if (values == null) {
            return object;
        }
        values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> object.set(entry.getKey(), canonicalValue(entry.getValue())));
        return object;
    }

    private JsonNode canonicalValue(Object value) {
        if (value == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (value instanceof String text) {
            return JsonNodeFactory.instance.textNode(text);
        }
        if (value instanceof Character character) {
            return JsonNodeFactory.instance.textNode(character.toString());
        }
        if (value instanceof Boolean bool) {
            return JsonNodeFactory.instance.booleanNode(bool);
        }
        if (value instanceof Byte number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof Short number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof Integer number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof Long number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof BigInteger number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof Float number) {
            return Float.isFinite(number)
                    ? JsonNodeFactory.instance.numberNode(number)
                    : JsonNodeFactory.instance.textNode(number.toString());
        }
        if (value instanceof Double number) {
            return Double.isFinite(number)
                    ? JsonNodeFactory.instance.numberNode(number)
                    : JsonNodeFactory.instance.textNode(number.toString());
        }
        if (value instanceof BigDecimal number) {
            return JsonNodeFactory.instance.numberNode(number);
        }
        if (value instanceof byte[] bytes) {
            return JsonNodeFactory.instance.binaryNode(bytes);
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return JsonNodeFactory.instance.binaryNode(bytes);
        }
        if (value instanceof Date date) {
            return JsonNodeFactory.instance.numberNode(date.getTime());
        }
        if (value instanceof TemporalAccessor temporal) {
            return JsonNodeFactory.instance.textNode(temporal.toString());
        }
        if (value instanceof Map<?, ?> map) {
            ObjectNode object = mapper.createObjectNode();
            List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
            entries.sort(Comparator.comparing(entry -> String.valueOf(entry.getKey())));
            for (Map.Entry<?, ?> entry : entries) {
                object.set(String.valueOf(entry.getKey()), canonicalValue(entry.getValue()));
            }
            return object;
        }
        if (value instanceof Collection<?> collection) {
            ArrayNode array = mapper.createArrayNode();
            collection.forEach(element -> array.add(canonicalValue(element)));
            return array;
        }
        if (value.getClass().isArray()) {
            throw new IllegalArgumentException(
                    "unsupported source partition/offset array type: " + value.getClass().getName());
        }
        if (value instanceof Enum<?> enumeration) {
            return JsonNodeFactory.instance.textNode(enumeration.name());
        }
        throw new IllegalArgumentException(
                "unsupported source partition/offset value type: " + value.getClass().getName());
    }

    private static JsonConverter converter(boolean isKey) {
        JsonConverter converter = new JsonConverter();
        converter.configure(
                Map.of("schemas.enable", true, "replace.null.with.default", false), isKey);
        return converter;
    }

    private static void putNullableText(ObjectNode node, String name, String value) {
        if (value == null) {
            node.putNull(name);
        }
        else {
            node.put(name, value);
        }
    }

    private static void putNullableInteger(ObjectNode node, String name, Integer value) {
        if (value == null) {
            node.putNull(name);
        }
        else {
            node.put(name, value);
        }
    }

    private static void putNullableLong(ObjectNode node, String name, Long value) {
        if (value == null) {
            node.putNull(name);
        }
        else {
            node.put(name, value);
        }
    }
}
