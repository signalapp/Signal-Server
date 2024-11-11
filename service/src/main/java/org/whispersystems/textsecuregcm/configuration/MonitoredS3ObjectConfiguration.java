/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@JsonTypeName("default")
public record MonitoredS3ObjectConfiguration(
    @NotBlank String s3Region,
    @NotBlank String s3Bucket,
    @NotBlank String objectKey,
    Long maxSize,
    Duration refreshInterval) implements S3ObjectMonitorFactory {

  private static final long DEFAULT_MAXSIZE = 16 * 1024 * 1024;
  private static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(5);

  public MonitoredS3ObjectConfiguration {
    if (maxSize == null) {
      maxSize = DEFAULT_MAXSIZE;
    }
    if (refreshInterval == null) {
      refreshInterval = DEFAULT_REFRESH_INTERVAL;
    }
  }

  @Override
  public S3ObjectMonitor build(final AwsCredentialsProvider awsCredentialsProvider,
      final ScheduledExecutorService refreshExecutorService) {

    return new S3ObjectMonitor(awsCredentialsProvider, s3Region, s3Bucket, objectKey, maxSize, refreshExecutorService,
        refreshInterval);
  }
}
