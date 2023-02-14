package org.whispersystems.textsecuregcm.spam;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.Optional;
import java.util.function.Function;

/**
 * Generates ReportSpamTokens to be used for spam reports.
 */
public interface ReportSpamTokenProvider {

  /**
   * Generate a new ReportSpamToken
   *
   * @param context the message request context
   * @return either a generated token or nothing
   */
  Optional<byte[]> makeReportSpamToken(ContainerRequestContext context);

  /**
   * Provider which generates nothing
   *
   * @return the provider
   */
  static ReportSpamTokenProvider noop() {
    return context -> Optional.empty();
  }
}
