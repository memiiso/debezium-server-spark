/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class DebeziumEvent {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEvent.class);
  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  protected final JsonNode valueSchema;
  protected final JsonNode keySchema;

  public DebeziumEvent(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
  }

  public String destination() {
    return destination;
  }

  public JsonNode value() {
    return value;
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode valueSchema() {
    return valueSchema;
  }

  public JsonNode keySchema() {
    return keySchema;
  }

}
