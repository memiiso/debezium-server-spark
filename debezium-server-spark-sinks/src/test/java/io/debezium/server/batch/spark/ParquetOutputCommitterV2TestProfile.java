/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class ParquetOutputCommitterV2TestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("io.debezium.server.batch", "DEBUG");
    config.put("debezium.sink.sparkbatch.spark.sql.parquet.output.committer.class", "io.debezium.server.batch.spark" +
        ".ParquetOutputCommitterV2");
    config.put("debezium.sink.sparkbatch.mapreduce.fileoutputcommitter.pending.dir", "_tmptest");
    return config;
  }
}