/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.shared;

import io.debezium.server.batch.BatchUtil;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest {
  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  String objectKeyPrefix;
  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "")
  String bucket;

  @BeforeAll
  static void setup() {
    Map<String, String> appSparkConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(BaseSparkTest.sparkconf::set);
    BaseSparkTest.sparkconf.set("spark.ui.enabled", "false");

    BaseSparkTest.spark = SparkSession
        .builder()
        .config(BaseSparkTest.sparkconf)
        .getOrCreate();
  }

  public static void PGCreateTestDataTable() throws Exception {
    // create test table
    String sql = "" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar VARCHAR" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
  }

  public static int PGLoadTestDataTable(int numRows) throws Exception {
    return PGLoadTestDataTable(numRows, false);
  }

  public static int PGLoadTestDataTable(int numRows, boolean addRandomDelay) throws Exception {
    int numInsert = 0;
    do {

      new Thread(() -> {
        try {
          if (addRandomDelay) {
            Thread.sleep(TestUtil.randomInt(20000, 100000));
          }
          String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
              "VALUES ";
          StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
          for (int i = 0; i < 200; i++) {
            values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
          }
          SourcePostgresqlDB.runSQL(sql + values);
          SourcePostgresqlDB.runSQL("COMMIT;");
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();

      numInsert += 200;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public static void mysqlCreateTestDataTable() throws Exception {
    // create test table
    String sql = "\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar TEXT\n" +
        "          );";
    SourceMysqlDB.runSQL(sql);
  }

  public static int mysqlLoadTestDataTable(int numRows) throws Exception {

    int numInsert = 0;
    do {
      String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
          "VALUES ";
      StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
      for (int i = 0; i < 10; i++) {
        values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
      }
      SourceMysqlDB.runSQL(sql + values);
      numInsert += 10;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public Dataset<Row> getTableData(String table) {
    return spark.read().option("mergeSchema", "true")
        .parquet(bucket + "/" + objectKeyPrefix + table + "/*")
        .withColumn("input_file", functions.input_file_name());
  }

}
