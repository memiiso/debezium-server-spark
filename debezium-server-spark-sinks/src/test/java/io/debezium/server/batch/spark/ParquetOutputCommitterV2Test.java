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
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ParquetOutputCommitterV2TestProfile.class)
class ParquetOutputCommitterV2Test extends BaseSparkTest {
  @Test
  public void testSimpleUpload() {
    Testing.Print.enable();

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.customers");
        df.show(false);
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        S3Minio.listFiles();
        return false;
      }
    });
  }

  @After
  public void onTearDown() {
    S3Minio.listFiles();
  }

}