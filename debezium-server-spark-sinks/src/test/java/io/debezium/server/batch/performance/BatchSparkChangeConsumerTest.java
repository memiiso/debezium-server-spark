/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.performance;

import io.debezium.server.batch.shared.S3Minio;
import io.debezium.server.batch.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(BatchSparkChangeConsumerTestProfile.class)
public class BatchSparkChangeConsumerTest extends BatchSparkChangeConsumerBaseTest {

  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "10000")
  Integer maxBatchSize;

  @Test
  @Disabled
  public void testPerformance() throws Exception {
    super.testPerformance(maxBatchSize);
  }

}
