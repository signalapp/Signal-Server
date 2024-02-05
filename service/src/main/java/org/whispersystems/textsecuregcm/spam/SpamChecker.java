/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.spam;

import org.whispersystems.textsecuregcm.storage.Account;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import java.util.Optional;

public interface SpamChecker {

  /**
   * Determine if a message may be spam
   *
   * @param requestContext   The request context for a message send attempt
   * @param maybeSource      The sender of the message, could be empty if this as message sent with sealed sender
   * @param maybeDestination The destination of the message, could be empty if the destination does not exist or could
   *                         not be retrieved
   * @return A response to return if the request is determined to be spam, otherwise empty if the message should be sent
   */
  Optional<Response> checkForSpam(
      final ContainerRequestContext requestContext,
      final Optional<Account> maybeSource,
      final Optional<Account> maybeDestination);

  static SpamChecker noop() {
    return (ignoredContext, ignoredSource, ignoredDestination) -> Optional.empty();
  }
}
