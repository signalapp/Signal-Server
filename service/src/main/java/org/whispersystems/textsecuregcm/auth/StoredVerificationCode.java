/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.MessageDigest;
import java.time.Duration;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.Util;

public record StoredVerificationCode(String code,
                                     long timestamp,
                                     String pushCode,
                                     @Nullable byte[] sessionId) {

  public static final Duration EXPIRATION = Duration.ofMinutes(10);

  public boolean isValid(String theirCodeString) {
    if (Util.isEmpty(code) || Util.isEmpty(theirCodeString)) {
      return false;
    }

    byte[] ourCode = code.getBytes();
    byte[] theirCode = theirCodeString.getBytes();

    return MessageDigest.isEqual(ourCode, theirCode);
  }
}
