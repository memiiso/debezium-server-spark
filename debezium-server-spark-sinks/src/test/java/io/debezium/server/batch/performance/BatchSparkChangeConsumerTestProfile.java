/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.performance;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class BatchSparkChangeConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "sparkbatch");
    config.put("debezium.source.max.batch.size", "24000");
    config.put("debezium.source.max.queue.size", "700000");
    config.put("debezium.source.table.include.list", "inventory.test_date_table,inventory.customers");
    config.put("quarkus.log.category.\"org.apache.spark.scheduler.TaskSetManager\".level", "ERROR");
    // 30000 30-second
    // config.put("debezium.source.poll.interval.ms", "30000");
    return config;
  }
}
