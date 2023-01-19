package org.whispersystems.textsecuregcm.abuse;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Handles ReportSpamTokens during spam reports.
 */
public interface ReportSpamTokenHandler {

  /**
   * Handle spam reports using the given ReportSpamToken and other provided parameters.
   *
   * @param reportSpamToken binary data representing a spam report token.
   * @return true if the token could be handled (and was), false otherwise.
   */
  CompletableFuture<Boolean> handle(
      Optional<String> sourceNumber,
      Optional<UUID> sourceAci,
      Optional<UUID> sourcePni,
      UUID messageGuid,
      UUID spamReporterUuid,
      byte[] reportSpamToken);

  /**
   * Handler which does nothing.
   *
   * @return the handler
   */
  static ReportSpamTokenHandler noop() {
    return new ReportSpamTokenHandler() {
      @Override
      public CompletableFuture<Boolean> handle(
          final Optional<String> sourceNumber,
          final Optional<UUID> sourceAci,
          final Optional<UUID> sourcePni,
          final UUID messageGuid,
          final UUID spamReporterUuid,
          final byte[] reportSpamToken) {
        return CompletableFuture.completedFuture(false);
      }
    };
  }


}
