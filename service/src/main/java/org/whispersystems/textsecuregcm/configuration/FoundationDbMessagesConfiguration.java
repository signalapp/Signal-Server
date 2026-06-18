/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;

public record FoundationDbMessagesConfiguration(@NotEmpty Map<String, @Valid FoundationDbDatabaseFactory> clusters,
                                                @NotEmpty Map<@PositiveOrZero @Max(FoundationDbMessageStore.MAX_EPOCHS - 1) Integer, @Size(min = 1, max = FoundationDbMessageStore.MAX_SHARDS - 1) List<String>> epochs,
                                                @PositiveOrZero @Max(FoundationDbMessageStore.MAX_EPOCHS - 1) int activeEpoch,
                                                @NotEmpty Map<@PositiveOrZero @Max(63) Integer, SecretBytes> versionstampCipherKeys,
                                                @PositiveOrZero @Max(63) int currentVersionstampCipherKey,
                                                @PositiveOrZero @Max(1_000_000) long maxWatchesPerClient) {

  public static final long DEFAULT_MAX_WATCHES_PER_CLIENT = 10_000;

  @AssertTrue
  boolean isEveryEpochClusterConfigured() {
    for (final List<String> clustersInEpoch : epochs().values()) {
      for (final String cluster : clustersInEpoch) {
        if (!clusters.containsKey(cluster)) {
          return false;
        }
      }
    }

    return true;
  }

  @AssertTrue
  boolean isEveryEpochFreeOfDuplicates() {
    for (final List<String> clustersInEpoch : epochs().values()) {
      if (new HashSet<>(clustersInEpoch).size() != clustersInEpoch.size()) {
        return false;
      }
    }

    return true;
  }

  @AssertTrue
  boolean isActiveEpochConfigured() {
    return epochs().containsKey(activeEpoch());
  }

  @AssertTrue
  boolean isCurrentVersionstampCipherKeyConfigured() {
    return versionstampCipherKeys().containsKey(currentVersionstampCipherKey());
  }
}
