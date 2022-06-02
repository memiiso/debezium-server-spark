/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.performance;

import io.debezium.server.batch.shared.BaseSparkTest;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BatchSparkChangeConsumerBaseTest extends BaseSparkTest {

  public void testPerformance(int maxBatchSize) throws Exception {

    int iteration = 10;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          PGLoadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_date_table");
        return df.count() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    Dataset<Row> df = getTableData("testc.inventory.test_date_table");
    System.out.println("Row Count=" + df.count());
  }

}
