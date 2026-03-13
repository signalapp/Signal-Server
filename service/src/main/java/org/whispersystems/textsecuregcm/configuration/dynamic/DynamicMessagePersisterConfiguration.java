/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;

public class DynamicMessagePersisterConfiguration {

  @JsonProperty
  private boolean persistenceEnabled = true;

  /**
   * If we have to trim a client's persisted queue to make room to persist from Redis to DynamoDB, how much extra room should we make
   */
  @JsonProperty
  private double trimOversizedQueueExtraRoomRatio = 1.5;

  @JsonProperty
  private Duration nodeClaimTtl = Duration.ofHours(1);

  @JsonProperty
  private Duration sleepBetweenNodes = Duration.ofSeconds(5);

  public DynamicMessagePersisterConfiguration() {}

  @VisibleForTesting
  public DynamicMessagePersisterConfiguration(final boolean persistenceEnabled,
      final double trimOversizedQueueExtraRoomRatio,
      final Duration nodeClaimTtl,
      final Duration sleepBetweenNodes) {

    this.persistenceEnabled = persistenceEnabled;
    this.trimOversizedQueueExtraRoomRatio = trimOversizedQueueExtraRoomRatio;
    this.nodeClaimTtl = nodeClaimTtl;
    this.sleepBetweenNodes = sleepBetweenNodes;
  }

  public boolean isPersistenceEnabled() {
    return persistenceEnabled;
  }

  public double getTrimOversizedQueueExtraRoomRatio() {
    return trimOversizedQueueExtraRoomRatio;
  }

  public Duration getNodeClaimTtl() {
    return nodeClaimTtl;
  }

  public Duration getSleepBetweenNodes() {
    return sleepBetweenNodes;
  }
}
