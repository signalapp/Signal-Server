/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import javax.validation.constraints.NotBlank;

public record MonitoredS3ObjectConfiguration(
    @NotBlank String s3Region,
    @NotBlank String s3Bucket,
    @NotBlank String objectKey,
    long maxSize,
    Duration refreshInterval
) {

  static long DEFAULT_MAXSIZE = 16*1024*1024;
  static Duration DEFAULT_DURATION = Duration.ofMinutes(5);

  public MonitoredS3ObjectConfiguration(String s3Region, String s3Bucket, String objectKey) {
    this(s3Region, s3Bucket, objectKey, DEFAULT_MAXSIZE, DEFAULT_DURATION);
  }
}
