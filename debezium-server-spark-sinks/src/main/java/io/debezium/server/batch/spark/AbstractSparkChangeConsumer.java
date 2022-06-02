/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.debezium.server.batch.AbstractChangeConsumer;
import io.debezium.server.batch.BatchUtil;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractSparkChangeConsumer extends AbstractChangeConsumer {

  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected SparkSession spark;
  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
  String bucket;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "parquet")
  String saveFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-mode", defaultValue = "append")
  String saveMode;
  @ConfigProperty(name = "debezium.sink.sparkbatch.cast-deleted-field", defaultValue = "false")
  Boolean castDeletedField;

  protected void stopSparkSession() {
    try {
      LOGGER.info("Closing Spark");
      if (!spark.sparkContext().isStopped()) {
        spark.close();
      }
      LOGGER.debug("Closed Spark");
    } catch (Exception e) {
      LOGGER.warn("Exception during Spark shutdown ", e);
    }
  }

  public void initizalize() throws InterruptedException {
    super.initizalize();

    Map<String, String> appSparkConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(this.sparkconf::set);
    this.sparkconf.set("spark.ui.enabled", "false");

    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Starting Spark Consumer({})", this.getClass().getSimpleName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
    LOGGER.info("Spark Version {}", this.spark.version());
    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());
  }

}
