/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.common;

import io.debezium.server.batch.DebeziumMetrics;
import io.debezium.server.batch.shared.BaseSparkTest;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxBatchSizeWaitTestProfile.class)
class MaxBatchSizeWaitTest extends BaseSparkTest {
  @Inject
  DebeziumMetrics debeziumMetrics;
  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  Integer pollIntervalMs;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;

  @Test
  public void testPerformance() throws Exception {
    debeziumMetrics.initizalize();
    int iteration = 100;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      PGLoadTestDataTable(maxBatchSize / 10, true);
    }
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_date_table");
        df.createOrReplaceTempView("test_date_table_batch_size");
        df = spark
            .sql("SELECT substring(input_file,101,36) as input_file, " +
                "count(*) as batch_size FROM test_date_table_batch_size group " +
                "by 1");
        df.show(false);
        return df.filter("batch_size = " + maxBatchSize).count() >= 5;
      } catch (Exception e) {
        return false;
      }
    });
  }

}