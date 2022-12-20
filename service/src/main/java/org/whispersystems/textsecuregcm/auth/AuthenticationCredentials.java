/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Hex;
import org.signal.libsignal.protocol.kdf.HKDF;
import org.whispersystems.textsecuregcm.util.Util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class AuthenticationCredentials {
  private static final String V2_PREFIX = "2.";

  private final String hashedAuthenticationToken;
  private final String salt;

  public enum Version {
    V1,
    V2,
  }

  public static final Version CURRENT_VERSION = Version.V2;

  public AuthenticationCredentials(String hashedAuthenticationToken, String salt) {
    this.hashedAuthenticationToken = hashedAuthenticationToken;
    this.salt                      = salt;
  }

  public AuthenticationCredentials(String authenticationToken) {
    this.salt = String.valueOf(Util.ensureNonNegativeInt(new SecureRandom().nextInt()));
    this.hashedAuthenticationToken = getV2HashedValue(salt, authenticationToken);
  }

  @VisibleForTesting
  public AuthenticationCredentials v1ForTesting(String authenticationToken) {
    String salt = String.valueOf(Util.ensureNonNegativeInt(new SecureRandom().nextInt()));
    return new AuthenticationCredentials(getV1HashedValue(salt, authenticationToken), salt);
  }

  public Version getVersion() {
    if (this.hashedAuthenticationToken.startsWith(V2_PREFIX)) {
      return Version.V2;
    }
    return Version.V1;
  }

  public String getHashedAuthenticationToken() {
    return hashedAuthenticationToken;
  }

  public String getSalt() {
    return salt;
  }

  public boolean verify(String authenticationToken) {
    final String theirValue = switch (getVersion()) {
      case V1 -> getV1HashedValue(salt, authenticationToken);
      case V2 -> getV2HashedValue(salt, authenticationToken);
    };
    return MessageDigest.isEqual(theirValue.getBytes(StandardCharsets.UTF_8), this.hashedAuthenticationToken.getBytes(StandardCharsets.UTF_8));
  }

  private static String getV1HashedValue(String salt, String token) {
    try {
      return new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest((salt + token).getBytes(StandardCharsets.UTF_8))));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static final byte[] AUTH_TOKEN_HKDF_INFO = "authtoken".getBytes(StandardCharsets.UTF_8);
  private static String getV2HashedValue(String salt, String token) {
    byte[] secret = HKDF.deriveSecrets(
        token.getBytes(StandardCharsets.UTF_8),  // key
        salt.getBytes(StandardCharsets.UTF_8),  // salt
        AUTH_TOKEN_HKDF_INFO,
        32);
    return V2_PREFIX + Hex.encodeHexString(secret);
  }
}
