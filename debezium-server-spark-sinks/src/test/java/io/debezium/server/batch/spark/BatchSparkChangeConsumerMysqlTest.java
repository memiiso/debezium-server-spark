/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.debezium.server.batch.shared.BaseSparkTest;
import io.debezium.server.batch.shared.S3Minio;
import io.debezium.server.batch.shared.SourceMysqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourceMysqlDB.class)
@TestProfile(BatchSparkChangeConsumerMysqlTestProfile.class)
public class BatchSparkChangeConsumerMysqlTest extends BaseSparkTest {


  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;

  @Test
  public void testTombstoneEvents() throws Exception {
    // create test table
    String sqlCreate = "CREATE TABLE IF NOT EXISTS inventory.test_delete_table (" +
        " c_id INTEGER ," +
        " c_id2 INTEGER ," +
        " c_data TEXT," +
        "  PRIMARY KEY (c_id, c_id2)" +
        " );";
    String sqlInsert =
        "INSERT INTO inventory.test_delete_table (c_id, c_id2, c_data ) " +
            "VALUES  (1,1,'data'),(1,2,'data'),(1,3,'data'),(1,4,'data') ;";
    String sqlDelete = "DELETE FROM inventory.test_delete_table where c_id = 1 ;";

    SourceMysqlDB.runSQL(sqlCreate);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_delete_table");
        df.show(false);
        return df.count() >= 12; // 4 insert 4 delete 4 insert!
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testSimpleUpload() {
    Testing.Print.enable();

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        df.show(false);
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
  }

}
