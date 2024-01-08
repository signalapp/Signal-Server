/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public final class HttpUtils {

  private HttpUtils() {
    // utility class
  }

  public static boolean isSuccessfulResponse(final int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  public static String queryParamString(final Collection<Map.Entry<String, String>> params) {
    final StringBuilder sb = new StringBuilder();
    if (params.isEmpty()) {
      return sb.toString();
    }
    sb.append("?");
    sb.append(params.stream()
        .map(e -> "%s=%s".formatted(
            URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8),
            URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8)))
        .collect(Collectors.joining("&")));
    return sb.toString();
  }
}
