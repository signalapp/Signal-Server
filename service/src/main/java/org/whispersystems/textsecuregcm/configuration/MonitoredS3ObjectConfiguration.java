/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;
import javax.validation.constraints.NotBlank;

public record MonitoredS3ObjectConfiguration(
    @NotBlank String s3Region,
    @NotBlank String s3Bucket,
    @NotBlank String objectKey,
    Long maxSize,
    Duration refreshInterval) {

  private static long DEFAULT_MAXSIZE = 16*1024*1024;
  private static Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(5);

  public MonitoredS3ObjectConfiguration {
    if (maxSize == null) {
      maxSize = DEFAULT_MAXSIZE;
    }
    if (refreshInterval == null) {
      refreshInterval = DEFAULT_REFRESH_INTERVAL;
    }
  }
}
