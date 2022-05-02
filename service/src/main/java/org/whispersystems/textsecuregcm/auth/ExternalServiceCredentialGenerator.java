/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.whispersystems.textsecuregcm.util.Util;

public class ExternalServiceCredentialGenerator {

  private final byte[] key;
  private final byte[] userIdKey;
  private final boolean usernameDerivation;
  private final boolean prependUsername;
  private final Clock clock;

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey) {
    this(key, userIdKey, true, true);
  }

  public ExternalServiceCredentialGenerator(byte[] key, boolean prependUsername) {
    this(key, new byte[0], false, prependUsername);
  }

  @VisibleForTesting
  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation) {
    this(key, userIdKey, usernameDerivation, true);
  }

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername) {
    this(key, userIdKey, usernameDerivation, prependUsername, Clock.systemUTC());
  }

  @VisibleForTesting
  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername, Clock clock) {
    this.key = key;
    this.userIdKey = userIdKey;
    this.usernameDerivation = usernameDerivation;
    this.prependUsername = prependUsername;
    this.clock = clock;
  }

  public ExternalServiceCredentials generateFor(String identity) {
    Mac mac = getMacInstance();
    String username = getUserId(identity, mac, usernameDerivation);
    long currentTimeSeconds = clock.millis() / 1000;
    String prefix = username + ":" + currentTimeSeconds;
    String output = Hex.encodeHexString(Util.truncate(getHmac(key, prefix.getBytes(), mac), 10));
    String token = (prependUsername ? prefix : currentTimeSeconds) + ":" + output;

    return new ExternalServiceCredentials(username, token);
  }

  private String getUserId(String number, Mac mac, boolean usernameDerivation) {
    if (usernameDerivation) return Hex.encodeHexString(Util.truncate(getHmac(userIdKey, number.getBytes(), mac), 10));
    else                    return number;
  }

  private Mac getMacInstance() {
    try {
      return Mac.getInstance("HmacSHA256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private byte[] getHmac(byte[] key, byte[] input, Mac mac) {
    try {
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(input);
    } catch (InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

}
