package org.whispersystems.textsecuregcm.abuse;

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
    return create(c -> Optional.empty());
  }

  /**
   * Provider which generates ReportSpamTokens using the given function
   *
   * @param fn function from message requests to optional tokens
   * @return the provider
   */
  static ReportSpamTokenProvider create(Function<ContainerRequestContext, Optional<byte[]>> fn) {
    return fn::apply;
  }
}
