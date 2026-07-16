/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;

/// @param transactionTimeout the time limit for a transaction, which may [contain multiple tries](https://forums.foundationdb.org/t/defaults-for-transaction-timeouts-and-retries/315/2).
///                           Note that this is independent of the FoundationDB server’s transaction hard limit of 5 seconds.
/// @param transactionRetryLimit the maximum number of retries permitted _within_ a transaction
/// @param batchPriorityTransactionTimeout the maximum time for a batch priority transaction. Should be greater than or equal to {@link transactionTimeout}
/// @param batchPriorityTransactionRetryLimit the maximum number of retries permitted _within_ a batch priority transaction
public record FoundationDbMessagesConfiguration(@NotEmpty Map<String, @Valid FoundationDbDatabaseFactory> clusters,
                                                @NotEmpty Map<@PositiveOrZero @Max(FoundationDbMessageStore.MAX_EPOCHS - 1) Integer, @Size(min = 1, max = FoundationDbMessageStore.MAX_SHARDS - 1) List<String>> epochs,
                                                @PositiveOrZero @Max(FoundationDbMessageStore.MAX_EPOCHS - 1) int activeEpoch,
                                                @NotEmpty Map<@PositiveOrZero @Max(63) Integer, SecretBytes> versionstampCipherKeys,
                                                @PositiveOrZero @Max(63) int currentVersionstampCipherKey,
                                                @PositiveOrZero @Max(1_000_000) long maxWatchesPerClient,
                                                @NotNull Duration transactionTimeout,
                                                @PositiveOrZero long transactionRetryLimit,
                                                @NotNull Duration batchPriorityTransactionTimeout,
                                                @PositiveOrZero long batchPriorityTransactionRetryLimit) {

  public static final long DEFAULT_MAX_WATCHES_PER_CLIENT = 10_000;
  public static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofSeconds(1);
  public static final long DEFAULT_TRANSACTION_RETRY_LIMIT = 3;

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
