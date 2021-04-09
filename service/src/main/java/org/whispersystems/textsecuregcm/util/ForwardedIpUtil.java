/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.apache.commons.lang3.StringUtils;
import java.util.Optional;

/**
 * Tools for working with chains of IP addresses in forwarding lists in HTTP headers.
 *
 * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP | MDN</a>
 */
public class ForwardedIpUtil {

  /**
   * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
   *
   * @param forwardedFor the value of an X-Forwarded-For header
   *
   * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
   * {@code forwardedFor} was null
   */
  public static Optional<String> getMostRecentProxy(final String forwardedFor) {
    return Optional.ofNullable(forwardedFor)
        .filter(StringUtils::isNotBlank)
        .map(proxies -> proxies.split(","))
        .map(proxyArray -> proxyArray[proxyArray.length - 1])
        .map(String::trim);
  }
}
