/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;

public record RequestAttributes(InetAddress remoteAddress,
                                @Nullable String userAgent,
                                @Nullable String acceptLanguageRaw,
                                List<Locale.LanguageRange> acceptLanguage) {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestAttributes.class);

  public RequestAttributes(InetAddress remoteAddress,
      @Nullable String userAgent,
      @Nullable String acceptLanguageRaw) {
    this(remoteAddress, userAgent, acceptLanguageRaw, parseAcceptLanguage(acceptLanguageRaw, userAgent));
  }

  private static List<Locale.LanguageRange> parseAcceptLanguage(final String acceptLanguageRaw, final String userAgent) {
    List<Locale.LanguageRange> acceptLanguages = Collections.emptyList();
    if (StringUtils.isNotBlank(acceptLanguageRaw)) {
      try {
        acceptLanguages = Locale.LanguageRange.parse(acceptLanguageRaw);
      } catch (final IllegalArgumentException e) {
        LOGGER.debug("Invalid Accept-Language header from User-Agent {}: {}", userAgent, acceptLanguageRaw, e);
      }
    }
    return acceptLanguages;
  }
}
