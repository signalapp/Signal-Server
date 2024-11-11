/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.spam;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import org.whispersystems.textsecuregcm.auth.AccountAndAuthenticatedDeviceHolder;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;

public interface SpamChecker {

  /**
   * A result from the spam checker that is one of:
   * <ul>
   *   <li>
   *     Message is determined to be spam, and a response is returned
   *   </li>
   *   <li>
   *     Message is not spam, and an optional spam token is returned
   *   </li>
   * </ul>
   */
  sealed interface SpamCheckResult {}

  record Spam(Response response) implements SpamCheckResult {}

  record NotSpam(Optional<byte[]> token) implements SpamCheckResult {
    public static final NotSpam EMPTY_TOKEN = new NotSpam(Optional.empty());
  }

  /**
   * Determine if a message may be spam
   *
   * @param requestContext   The request context for a message send attempt
   * @param maybeSource      The sender of the message, could be empty if this as message sent with sealed sender
   * @param maybeDestination The destination of the message, could be empty if the destination does not exist or could
   *                         not be retrieved
   * @return A {@link SpamCheckResult}
   */
  SpamCheckResult checkForSpam(
      final ContainerRequestContext requestContext,
      final Optional<? extends AccountAndAuthenticatedDeviceHolder> maybeSource,
      final Optional<Account> maybeDestination,
      final Optional<ServiceIdentifier> maybeDestinationIdentifier);

  static SpamChecker noop() {
    return (ignoredContext, ignoredSource, ignoredDestination, ignoredDestinationIdentifier) -> NotSpam.EMPTY_TOKEN;
  }
}
