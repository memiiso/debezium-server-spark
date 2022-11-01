/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.TestConfigSource;
import io.debezium.server.batch.shared.S3Minio;
import io.debezium.server.batch.shared.SourcePostgresqlDB;

public class ConfigSource extends TestConfigSource {

  public static final String S3_REGION = "us-east-1";
  public static final String S3_BUCKET = "test-bucket";

  public ConfigSource() {

    config.put("quarkus.profile", "postgresql");
    // DEBEZIUM SOURCE conf
    config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    config.put("debezium.source.include.schema.changes", "false");
    config.put("debezium.source.decimal.handling.mode", "double");
    config.put("debezium.source.max.batch.size", "100");
    config.put("debezium.source.poll.interval.ms", "5000");
    //
    //config.put("debezium.source.offset.storage.file.filename", OFFSET_STORE_PATH.toAbsolutePath().toString());
    //config.put("debezium.source.offset.storage","org.apache.kafka.connect.storage.FileOffsetBackingStore");
    config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
    config.put("debezium.source.offset.flush.interval.ms", "60000");
    //
    config.put("debezium.source.database.history.kafka.bootstrap.servers", "kafka:9092");
    config.put("debezium.source.database.history.kafka.topic", "dbhistory.fullfillment");
    config.put("debezium.source.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
    //config.put("debezium.source.database.history.file.filename", HISTORY_FILE.toAbsolutePath().toString());
    config.put("debezium.source.database.hostname", SourcePostgresqlDB.POSTGRES_HOST);
    config.put("debezium.source.database.port", Integer.toString(5432));
    config.put("debezium.source.database.user", SourcePostgresqlDB.POSTGRES_USER);
    config.put("debezium.source.database.password", SourcePostgresqlDB.POSTGRES_PASSWORD);
    config.put("debezium.source.database.dbname", SourcePostgresqlDB.POSTGRES_DBNAME);
    config.put("debezium.source.database.server.name", "testc");
    config.put("%mysql.debezium.source.database.include.list", "inventory");
    config.put("%postgresql.debezium.source.schema.include.list", "inventory");
    config.put("debezium.source.table.include.list", "inventory.customers,inventory.orders,inventory.products," +
        "inventory.products_on_hand,inventory.geom," +
        "inventory.test_date_table,inventory.table_datatypes,inventory.test_delete_table");
    config.put("debezium.source.snapshot.select.statement.overrides.inventory.products_on_hand", "SELECT * FROM products_on_hand WHERE 1>2");
    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");

    // common sink conf
    config.put("debezium.sink.type", "sparkbatch");
    config.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    config.put("debezium.sink.batch.objectkey-partition", "true");
    // spark conf
    config.put("debezium.sink.sparkbatch.save-format", "parquet");
    config.put("debezium.sink.sparkbatch.bucket-name", "s3a://" + S3_BUCKET);
    config.put("debezium.sink.sparkbatch.spark.ui.enabled", "false");
    config.put("debezium.sink.sparkbatch.spark.sql.session.timeZone", "UTC");
    config.put("debezium.sink.sparkbatch.user.timezone", "UTC");
    config.put("debezium.sink.sparkbatch.spark.io.compression.codec", "snappy");
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.access.key", S3Minio.MINIO_ACCESS_KEY);
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.secret.key", S3Minio.MINIO_SECRET_KEY);
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.path.style.access", "true");
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"); // minio specific setting
    //config.put("debezium.sink.sparkbatch.spark.sql.parquet.output.committer.class", "io.debezium.server.batch.spark.ParquetOutputCommitterV2");
    //config.put("debezium.sink.sparkbatch.mapreduce.fileoutputcommitter.pending.dir", "_tmptest");


    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.lsn,source.txId");
    config.put("%mysql.debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.file,source.pos,source.row,source.gtid");
    config.put("%mysql.debezium.source.internal.implementation", "legacy");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");

    // logging levels
    config.put("quarkus.log.level", "INFO");
    config.put("quarkus.log.category.\"org.apache.spark\".level", "WARN");
    config.put("quarkus.log.category.\"org.apache.hadoop\".level", "ERROR");
    config.put("quarkus.log.category.\"org.apache.parquet\".level", "WARN");
    config.put("quarkus.log.category.\"org.eclipse.jetty\".level", "WARN");
    config.put("quarkus.log.category.\"io.debezium.server.batch.spark\".level", "INFO");

  }

  @Override
  public int getOrdinal() {
    // Configuration property precedence is based on ordinal values and since we override the
    // properties in TestConfigSource, we should give this a higher priority.
    return super.getOrdinal() + 1;
  }
}
