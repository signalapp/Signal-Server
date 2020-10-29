/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class AuthenticationCredentials {

  private final String hashedAuthenticationToken;
  private final String salt;

  public AuthenticationCredentials(String hashedAuthenticationToken, String salt) {
    this.hashedAuthenticationToken = hashedAuthenticationToken;
    this.salt                      = salt;
  }

  public AuthenticationCredentials(String authenticationToken) {
    this.salt                      = String.valueOf(Math.abs(new SecureRandom().nextInt()));
    this.hashedAuthenticationToken = getHashedValue(salt, authenticationToken);
  }

  public String getHashedAuthenticationToken() {
    return hashedAuthenticationToken;
  }

  public String getSalt() {
    return salt;
  }

  public boolean verify(String authenticationToken) {
    String theirValue = getHashedValue(salt, authenticationToken);
    return MessageDigest.isEqual(theirValue.getBytes(StandardCharsets.UTF_8), this.hashedAuthenticationToken.getBytes(StandardCharsets.UTF_8));
  }

  private static String getHashedValue(String salt, String token) {
    try {
      return new String(Hex.encodeHex(MessageDigest.getInstance("SHA1").digest((salt + token).getBytes(StandardCharsets.UTF_8))));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

}
