/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Tools for working with chains of IP addresses in forwarding lists in HTTP headers.
 *
 * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP | MDN</a>
 */
public final class ForwardedIpUtil {

  private ForwardedIpUtil() {
    // utility class
  }

  /**
   * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
   *
   * @param forwardedFor the value of an X-Forwarded-For header
   *
   * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
   * {@code forwardedFor} was null
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
