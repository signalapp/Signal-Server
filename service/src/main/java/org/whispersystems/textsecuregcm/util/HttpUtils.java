/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

public final class HttpUtils {

  private HttpUtils() {
    // utility class
  }

  public static boolean isSuccessfulResponse(final int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }
}
