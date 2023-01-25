/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

public final class HeaderUtils {

  public static final String X_SIGNAL_AGENT = "X-Signal-Agent";

  public static final String X_SIGNAL_KEY = "X-Signal-Key";

  public static final String TIMESTAMP_HEADER = "X-Signal-Timestamp";

  private HeaderUtils() {
    // utility class
  }

  public static String basicAuthHeader(final ExternalServiceCredentials credentials) {
    return basicAuthHeader(credentials.username(), credentials.password());
  }

  public static String basicAuthHeader(final String username, final String password) {
    requireNonNull(username);
    requireNonNull(password);
    return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static String getTimestampHeader() {
    return TIMESTAMP_HEADER + ":" + System.currentTimeMillis();
  }

  /**
   * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
   *
   * @param forwardedFor the value of an X-Forwarded-For header
   *
   * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
   * {@code forwardedFor} was null
   *
   * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP | MDN</a>
   */
  @Nonnull
  public static Optional<String> getMostRecentProxy(@Nullable final String forwardedFor) {
    return Optional.ofNullable(forwardedFor)
        .map(ff -> {
          final int idx = forwardedFor.lastIndexOf(',') + 1;
          return idx < forwardedFor.length()
              ? forwardedFor.substring(idx).trim()
              : null;
        })
        .filter(StringUtils::isNotBlank);
  }
}
