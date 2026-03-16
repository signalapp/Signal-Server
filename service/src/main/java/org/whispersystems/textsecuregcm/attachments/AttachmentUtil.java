/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.attachments;

import java.security.SecureRandom;
import java.util.Base64;

public class AttachmentUtil {
  public static final String CDN3_EXPERIMENT_NAME = "cdn3";

  private AttachmentUtil() {}

  public static String generateAttachmentKey(final SecureRandom secureRandom) {
    final byte[] bytes = new byte[15];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().encodeToString(bytes);
  }
}
