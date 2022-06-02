/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.batch.spark.DebeziumSparkEvent;
import io.debezium.util.Testing;

import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BatchUtilTest {

  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

  public StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
    JsonNode jsonNode = new ObjectMapper().readTree(event);

    DebeziumEvent e = new DebeziumEvent("test",
        jsonNode.get("payload"),
        null,
        jsonNode.get("schema"),
        null
    );
    return new DebeziumSparkEvent(e).getSparkDfSchema();
  }

  @Test
  public void testSimpleSchema() throws JsonProcessingException {
    StructType s = getEventSparkDfSchema(unwrapWithSchema);
    assertNotNull(s);
    assertTrue(s.catalogString().contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string"));
  }

  @Test
  public void testNestedSparkSchema() throws JsonProcessingException {
    StructType s = getEventSparkDfSchema(serdeWithSchema);
    assertNotNull(s);
    assertTrue(s.catalogString().contains("before:struct<id"));
    assertTrue(s.catalogString().contains("after:struct<id"));
  }

  @Test
  public void testValuePayloadWithSchemaAsJsonNode() {
    // testing Debezium deserializer
    final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    valueSerde.configure(Collections.emptyMap(), false);
    JsonNode deserializedData = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    assertEquals(deserializedData.getClass().getSimpleName(), "ObjectNode");
    assertTrue(deserializedData.has("after"));
    assertTrue(deserializedData.has("op"));
    assertTrue(deserializedData.has("before"));
    assertFalse(deserializedData.has("schema"));

    valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
    JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    assertFalse(deserializedSchema.has("schema"));
  }

}
