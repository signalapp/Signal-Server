/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;

public record VirtualThreadConfiguration(
    Duration pinEventThreshold,
    Integer maxConcurrentThreadsPerExecutor) {

  public VirtualThreadConfiguration() {
    this(null, null);
  }

  public VirtualThreadConfiguration {
    if (maxConcurrentThreadsPerExecutor == null) {
      maxConcurrentThreadsPerExecutor = 1_000_000;
    }
    if (pinEventThreshold == null) {
      pinEventThreshold = Duration.ofMillis(1);
    }
  }
}
