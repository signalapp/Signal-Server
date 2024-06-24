package org.whispersystems.textsecuregcm.spam;

import org.whispersystems.textsecuregcm.auth.AccountAndAuthenticatedDeviceHolder;
import org.whispersystems.textsecuregcm.storage.Account;
import javax.ws.rs.container.ContainerRequestContext;
import java.util.Optional;

/**
 * Generates ReportSpamTokens to be used for spam reports.
 */
public interface ReportSpamTokenProvider {

  /**
   * Generate a new ReportSpamToken
   *
   * @param context          the message request context
   * @param sender           the account that sent the unsealed sender message
   * @param maybeDestination the intended recepient of the message if available
   * @return either a generated token or nothing
   */
  Optional<byte[]> makeReportSpamToken(ContainerRequestContext context, final AccountAndAuthenticatedDeviceHolder sender,
      final Optional<Account> maybeDestination);

  /**
   * Provider which generates nothing
   *
   * @return the provider
   */
  static ReportSpamTokenProvider noop() {
    return (ignoredContext, ignoredSender, ignoredDest) -> Optional.empty();
  }
}
