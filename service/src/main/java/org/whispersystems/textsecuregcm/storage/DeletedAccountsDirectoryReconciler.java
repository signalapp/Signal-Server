/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest.User;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;

public class DeletedAccountsDirectoryReconciler {

  private final Logger logger = LoggerFactory.getLogger(DeletedAccountsDirectoryReconciler.class);

  private final DirectoryReconciliationClient directoryReconciliationClient;

  private final Timer deleteTimer;
  private final Counter errorCounter;

  public DeletedAccountsDirectoryReconciler(
      final String replicationName,
      final DirectoryReconciliationClient directoryReconciliationClient) {
    this.directoryReconciliationClient = directoryReconciliationClient;

    deleteTimer = Timer.builder(name(DeletedAccountsDirectoryReconciler.class, "delete"))
        .tag("replicationName", replicationName)
        .register(Metrics.globalRegistry);

    errorCounter = Counter.builder(name(DeletedAccountsDirectoryReconciler.class, "error"))
        .tag("replicationName", replicationName)
        .register(Metrics.globalRegistry);

  }

  public void onCrawlChunk(final List<User> deletedUsers) throws ChunkProcessingFailedException {

    try {
      deleteTimer.recordCallable(() -> {
        try {
          final DirectoryReconciliationResponse response = directoryReconciliationClient.delete(new DirectoryReconciliationRequest(null, null, deletedUsers));

          if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
            errorCounter.increment();

            throw new ChunkProcessingFailedException("Response status: " + response.getStatus());
          }
        } catch (final Exception e) {

          errorCounter.increment();

          throw new ChunkProcessingFailedException(e);
        }

        return null;
      });
    } catch (final ChunkProcessingFailedException e) {
      throw e;
    } catch (final Exception e) {
      logger.warn("Unexpected exception", e);
      throw new RuntimeException(e);
    }
  }

}
