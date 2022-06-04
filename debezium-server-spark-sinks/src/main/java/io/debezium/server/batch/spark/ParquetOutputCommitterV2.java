/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetOutputCommitterV2 extends FileOutputCommitterV2 {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputCommitterV2.class);

  private final Path outputPath;

  public ParquetOutputCommitterV2(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  // TODO: This method should propagate errors, and we should clean up
  // TODO: all the catching of Exceptions below -- see PARQUET-383
  public static void writeMetaDataFile(Configuration configuration, Path outputPath) {
    JobSummaryLevel level = ParquetOutputFormat.getJobSummaryLevel(configuration);
    if (level == JobSummaryLevel.NONE) {
      return;
    }

    try {
      final FileSystem fileSystem = outputPath.getFileSystem(configuration);
      FileStatus outputStatus = fileSystem.getFileStatus(outputPath);
      List<Footer> footers;

      switch (level) {
        case ALL:
          footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus, false); // don't skip row groups
          break;
        case COMMON_ONLY:
          footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus, true); // skip row groups
          break;
        default:
          throw new IllegalArgumentException("Unrecognized job summary level: " + level);
      }

      // If there are no footers, _metadata file cannot be written since there is no way to determine schema!
      // Onus of writing any summary files lies with the caller in this case.
      if (footers.isEmpty()) {
        return;
      }

      try {
        ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers, level);
      } catch (Exception e) {
        LOG.warn("could not write summary file(s) for " + outputPath, e);

        final Path metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE);

        try {
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true);
          }
        } catch (Exception e2) {
          LOG.warn("could not delete metadata file" + outputPath, e2);
        }

        try {
          final Path commonMetadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE);
          if (fileSystem.exists(commonMetadataPath)) {
            fileSystem.delete(commonMetadataPath, true);
          }
        } catch (Exception e2) {
          LOG.warn("could not delete metadata file" + outputPath, e2);
        }

      }
    } catch (Exception e) {
      LOG.warn("could not write summary file for " + outputPath, e);
    }
  }

  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    Configuration configuration = ContextUtil.getConfiguration(jobContext);
    writeMetaDataFile(configuration, outputPath);
  }
}
