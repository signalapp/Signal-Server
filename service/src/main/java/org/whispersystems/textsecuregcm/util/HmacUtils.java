/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class HmacUtils {

  private static final HexFormat HEX = HexFormat.of();

  private static final String HMAC_SHA_256 = "HmacSHA256";

  private static final ThreadLocal<Mac> THREAD_LOCAL_HMAC_SHA_256 = ThreadLocal.withInitial(() -> {
    try {
      return Mac.getInstance(HMAC_SHA_256);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  });

  private static Mac initializedThreadLocalMac(final byte[] key) {
    try {
      final Mac mac = THREAD_LOCAL_HMAC_SHA_256.get();
      mac.init(new SecretKeySpec(key, HMAC_SHA_256));
      return mac;
    } catch (final InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] hmac256(final byte[] key, final byte[] input) {
    return initializedThreadLocalMac(key).doFinal(input);
  }

  public static byte[] hmac256(final byte[] key, final byte[]... inputs) {
      final Mac mac = initializedThreadLocalMac(key);
      for (byte[] input : inputs) {
        mac.update(input);
      }
      return mac.doFinal();
  }

  public static byte[] hmac256(final byte[] key, final String input) {
    return hmac256(key, input.getBytes(StandardCharsets.UTF_8));
  }

  public static String hmac256ToHexString(final byte[] key, final byte[] input) {
    return HEX.formatHex(hmac256(key, input));
  }

  public static String hmac256ToHexString(final byte[] key, final String input) {
    return hmac256ToHexString(key, input.getBytes(StandardCharsets.UTF_8));
  }

  public static byte[] hmac256Truncated(final byte[] key, final byte[] input, final int length) {
    return Util.truncate(hmac256(key, input), length);
  }

  public static byte[] hmac256Truncated(final byte[] key, final String input, final int length) {
    return hmac256Truncated(key, input.getBytes(StandardCharsets.UTF_8), length);
  }

  public static String hmac256TruncatedToHexString(final byte[] key, final byte[] input, final int length) {
    return HEX.formatHex(Util.truncate(hmac256(key, input), length));
  }

  public static String hmac256TruncatedToHexString(final byte[] key, final String input, final int length) {
    return hmac256TruncatedToHexString(key, input.getBytes(StandardCharsets.UTF_8), length);
  }

  public static boolean hmacHexStringsEqual(final String expectedAsHexString, final String actualAsHexString) {
    try {
      final byte[] aBytes = HEX.parseHex(expectedAsHexString);
      final byte[] bBytes = HEX.parseHex(actualAsHexString);
      return MessageDigest.isEqual(aBytes, bBytes);
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }
}
