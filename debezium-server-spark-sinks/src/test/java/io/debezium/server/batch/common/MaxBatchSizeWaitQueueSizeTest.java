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
import io.debezium.server.batch.shared.SourcePostgresqlDB;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.sql.SQLException;
import java.time.Duration;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static io.debezium.server.batch.shared.TestUtil.randomInt;
import static io.debezium.server.batch.shared.TestUtil.randomString;

@QuarkusTest
@TestProfile(MaxBatchSizeWaitQueueSizeTestProfile.class)
@Disabled
class MaxBatchSizeWaitQueueSizeTest extends BaseSparkTest {
  @Inject
  DebeziumMetrics debeziumMetrics;
  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  Integer pollIntervalMs;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;


  public void insertData() throws SQLException, ClassNotFoundException {
    new Thread(() -> {
      try {
        for (int j = 0; j <= 999; j++) {
          Thread.sleep(randomInt(4000, 9000));
          String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
              "VALUES ";
          StringBuilder values =
              new StringBuilder("\n(" + randomInt(15, 32) + ", '" + randomString(5) + "', '" + randomString(10) + "')");
          values.append("\n,(").append(randomInt(15, 32)).append(", '").append(randomString(5)).append("', '").append(randomString(10)).append("')");
          SourcePostgresqlDB.runSQL(sql + values);
          SourcePostgresqlDB.runSQL("COMMIT;");
        }
      } catch (SQLException | ClassNotFoundException | InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
  }

  @Test
  public void testPerformance() throws Exception {
    debeziumMetrics.initizalize();
    PGCreateTestDataTable();
    this.insertData();
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_date_table");
        df.createOrReplaceTempView("test_date_table_batch_size");
        df = spark
            .sql("SELECT substring(input_file,101,36) as input_file, " +
                "count(*) as batch_size FROM test_date_table_batch_size group " +
                "by 1");
        //df.show(false);
        return df.filter("batch_size = " + maxBatchSize).count() >= 5;
      } catch (Exception e) {
        return false;
      }
    });
  }

}