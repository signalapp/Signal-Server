/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import org.signal.libsignal.protocol.kdf.HKDF;

public record SaltedTokenHash(String hash, String salt) {

  private static final String V2_PREFIX = "2.";

  private static final byte[] AUTH_TOKEN_HKDF_INFO = "authtoken".getBytes(StandardCharsets.UTF_8);

  private static final int SALT_SIZE = 16;

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();


  public static SaltedTokenHash generateFor(final String token) {
    final String salt = generateSalt();
    final String hash = calculateHash(salt, token);
    return new SaltedTokenHash(hash, salt);
  }

  public boolean verify(final String token) {
    return MessageDigest.isEqual(
        calculateHash(salt, token).getBytes(StandardCharsets.UTF_8),
        hash.getBytes(StandardCharsets.UTF_8));
  }

  private static String generateSalt() {
    final byte[] salt = new byte[SALT_SIZE];
    SECURE_RANDOM.nextBytes(salt);
    return HexFormat.of().formatHex(salt);
  }

  private static String calculateHash(final String salt, final String token) {
    final byte[] secret = HKDF.deriveSecrets(
        token.getBytes(StandardCharsets.UTF_8),  // key
        salt.getBytes(StandardCharsets.UTF_8),  // salt
        AUTH_TOKEN_HKDF_INFO,
        32);
    return V2_PREFIX + HexFormat.of().formatHex(secret);
  }
}
