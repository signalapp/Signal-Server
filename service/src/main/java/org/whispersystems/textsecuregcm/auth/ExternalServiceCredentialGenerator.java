/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.whispersystems.textsecuregcm.util.Util;

public class ExternalServiceCredentialGenerator {

  private final byte[] key;
  private final byte[] userIdKey;
  private final boolean usernameDerivation;
  private final boolean prependUsername;
  private final boolean truncateKey;
  private final Clock clock;

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey) {
    this(key, userIdKey, true, true, true);
  }

  public ExternalServiceCredentialGenerator(byte[] key, boolean prependUsername) {
    this(key, prependUsername, true);
  }

  public ExternalServiceCredentialGenerator(byte[] key, boolean prependUsername, boolean truncateKey) {
    this(key, new byte[0], false, prependUsername, truncateKey);
  }

  @VisibleForTesting
  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation) {
    this(key, userIdKey, usernameDerivation, true, true);
  }

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername) {
    this(key, userIdKey, usernameDerivation, prependUsername, true, Clock.systemUTC());
  }

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername, boolean truncateKey) {
    this(key, userIdKey, usernameDerivation, prependUsername, truncateKey, Clock.systemUTC());
  }

  @VisibleForTesting
  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername, Clock clock) {
    this(key, userIdKey, usernameDerivation, prependUsername, true, clock);
  }

  @VisibleForTesting
  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation,
      boolean prependUsername, boolean truncateKey, Clock clock) {
    this.key = key;
    this.userIdKey = userIdKey;
    this.usernameDerivation = usernameDerivation;
    this.prependUsername = prependUsername;
    this.truncateKey = truncateKey;
    this.clock = clock;
  }

  public ExternalServiceCredentials generateFor(String identity) {
    Mac mac = getMacInstance();
    String username = getUserId(identity, mac, usernameDerivation);
    long currentTimeSeconds = clock.millis() / 1000;
    String prefix = username + ":" + currentTimeSeconds;
    byte[] prefixMac = getHmac(key, prefix.getBytes(), mac);
    final HexFormat hex = HexFormat.of();
    String output = hex.formatHex(truncateKey ? Util.truncate(prefixMac, 10) : prefixMac);
    String token = (prependUsername ? prefix : currentTimeSeconds) + ":" + output;

    return new ExternalServiceCredentials(username, token);
  }

  private String getUserId(String number, Mac mac, boolean usernameDerivation) {
    final HexFormat hex = HexFormat.of();
    if (usernameDerivation) return hex.formatHex(Util.truncate(getHmac(userIdKey, number.getBytes(), mac), 10));
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
