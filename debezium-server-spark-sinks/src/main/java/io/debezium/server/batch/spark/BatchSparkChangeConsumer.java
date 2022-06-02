/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.debezium.server.batch.DebeziumEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static org.apache.spark.sql.functions.col;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkbatch")
@Dependent
public class BatchSparkChangeConsumer extends AbstractSparkChangeConsumer {

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition", defaultValue = "false")
  protected Boolean partitionData;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition-time-zone", defaultValue = "UTC")
  protected String partitionDataZone;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  protected Optional<String> objectKeyPrefix;

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }


  private String writeJsonNodeAsString(JsonNode e) {
    try {
      return mapper.writeValueAsString(e);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long uploadDestination(String destination, List<DebeziumEvent> data) {

    Instant start = Instant.now();
    StructType dfSchema = new DebeziumSparkEvent(data.get(0)).getSparkDfSchema();
    List<String> dataList = data.stream().map(e -> writeJsonNodeAsString(e.value())).collect(Collectors.toList());
    Dataset<String> ds = spark.createDataset(dataList, Encoders.STRING());
    Dataset<Row> df;

    if (dfSchema != null) {
      LOGGER.debug("Reading data with schema definition, Schema:\n{}", dfSchema);
      df = spark.read().schema(dfSchema).json(ds);
    } else {
      LOGGER.debug("Reading data without schema definition");
      df = spark.read().json(ds);
    }

    if (castDeletedField) {
      df = df.withColumn("__deleted", col("__deleted").cast(DataTypes.BooleanType));
    }

    long numRecords;

    String uploadFile = map(destination);
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .save(bucket + "/" + uploadFile);

      numRecords = df.count();
      LOGGER.debug("Uploaded {} rows to:'{}' upload time:{}, ",
          numRecords,
          uploadFile,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
      );
    }

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("uploadDestination df row val:{}", x.toString())
      );
    }
    df.unpersist();
    return numRecords;
  }


  private String getPartition() {
    final ZonedDateTime batchTime = ZonedDateTime.now(ZoneId.of(partitionDataZone));
    return "dt=" + batchTime.getYear()
        + "-" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0')
        + "-" + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
  }

  public String map(String destination) {
    Objects.requireNonNull(destination, "destination Cannot be Null");
    if (partitionData) {
      String partitioned = getPartition();
      return objectKeyPrefix.orElse("") +
          destination.replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse("")) +
          "/" + partitioned;
    } else {
      return objectKeyPrefix + destination.replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""));
    }
  }

}