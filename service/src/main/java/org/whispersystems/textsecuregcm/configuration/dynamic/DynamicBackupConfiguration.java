/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import io.dropwizard.util.DataSize;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/**
 *
 * @param deletionConcurrency How many cdn object deletion requests can be outstanding at a time per backup deletion operation
 * @param copyConcurrency How many cdn object copy requests can be outstanding at a time per batch copy-to-backup operation
 * @param usageCheckpointCount When doing batch operations, how often persist usage deltas
 * @param maxQuotaStaleness The maximum age of a quota estimate that can be used to enforce a quota limit
 * @param maxTotalMediaSize The number of media bytes a paid-tier user may store
 */
public record DynamicBackupConfiguration(
  @NotNull Integer deletionConcurrency,
  @NotNull Integer copyConcurrency,
  @NotNull Integer usageCheckpointCount,
  @NotNull Duration maxQuotaStaleness,
  @NotNull Long maxTotalMediaSize) {

  public DynamicBackupConfiguration {
    if (deletionConcurrency == null) {
      deletionConcurrency = 10;
    }
    if (copyConcurrency == null) {
      copyConcurrency = 10;
    }
    if (usageCheckpointCount == null) {
      usageCheckpointCount = 10;
    }
    if (maxQuotaStaleness == null) {
      maxQuotaStaleness = Duration.ofSeconds(10);
    }
    if (maxTotalMediaSize == null) {
      maxTotalMediaSize = DataSize.gibibytes(100).toBytes();
    }
  }

  public DynamicBackupConfiguration() {
    this(null, null, null, null, null);
  }
}
