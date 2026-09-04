package dev.dogpaddle.experiments.debeziumd1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class DeliveryCodecTest {
    @Test
    void preserves_source_identity_and_connect_key_value_schemas() throws Exception {
        Map<String, Object> partition = new LinkedHashMap<>();
        partition.put("server", "inventory");
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("txId", 42L);
        offset.put("lsn", 9_007_199_254_740_993L);

        Schema keySchema = SchemaBuilder.struct()
                .name("public.items.Key")
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("public.items.Envelope")
                .field("op", Schema.STRING_SCHEMA)
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Struct key = new Struct(keySchema).put("id", 5L);
        Struct value = new Struct(valueSchema).put("op", "c").put("id", 5L);
        SourceRecord record = new SourceRecord(
                partition,
                offset,
                "inventory.public.items",
                0,
                keySchema,
                key,
                valueSchema,
                value,
                123L);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode delivery = mapper.readTree(new DeliveryCodec(mapper).encode(3, List.of(record)));

        assertEquals("delivery", delivery.path("kind").asText());
        assertEquals(3, delivery.path("token").asLong());
        assertEquals(1, delivery.path("event_count").asInt());
        assertEquals("inventory", delivery.path("partition").path("server").asText());
        assertEquals("9007199254740993", delivery.path("offset").path("lsn").asText());
        JsonNode event = delivery.path("events").path(0);
        assertEquals("inventory.public.items", event.path("topic").asText());
        assertEquals(5, event.path("key").path("payload").path("id").asLong());
        assertEquals("public.items.Key", event.path("key").path("schema").path("name").asText());
        assertEquals("c", event.path("value").path("payload").path("op").asText());
        assertEquals(
                "public.items.Envelope",
                event.path("value").path("schema").path("name").asText());
        assertTrue(event.has("partition"));
        assertTrue(event.has("offset"));
        assertFalse(event.path("value").path("schema").isMissingNode());
    }

    @Test
    void preserves_explicit_null_instead_of_substituting_the_schema_default() throws Exception {
        Schema fieldSchema = SchemaBuilder.string()
                .optional()
                .defaultValue("schema-default")
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("public.items.NullableDefault")
                .field("description", fieldSchema)
                .build();
        Struct value = new Struct(valueSchema).put("description", null);
        SourceRecord record = new SourceRecord(
                Map.of("server", "inventory"),
                Map.of("lsn", 1L),
                "inventory.public.items",
                null,
                null,
                null,
                valueSchema,
                value);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode delivery = mapper.readTree(new DeliveryCodec(mapper).encode(4, List.of(record)));
        JsonNode encoded = delivery.path("events").path(0).path("value");

        assertEquals(
                "schema-default",
                encoded.path("schema").path("fields").path(0).path("default").asText());
        assertTrue(encoded.path("payload").has("description"));
        assertNull(encoded.path("payload").get("description").textValue());
        assertTrue(encoded.path("payload").path("description").isNull());
    }
}
