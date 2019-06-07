package org.whispersystems.textsecuregcm.auth;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

public class ExternalServiceCredentialGenerator {

  private final Logger logger = LoggerFactory.getLogger(ExternalServiceCredentialGenerator.class);

  private final byte[] key;
  private final byte[] userIdKey;
  private final boolean usernameDerivation;

  public ExternalServiceCredentialGenerator(byte[] key, byte[] userIdKey, boolean usernameDerivation) {
    this.key                = key;
    this.userIdKey          = userIdKey;
    this.usernameDerivation = usernameDerivation;
  }

  public ExternalServiceCredentials generateFor(String number) {
    Mac    mac                = getMacInstance();
    String username           = getUserId(number, mac, usernameDerivation);
    long   currentTimeSeconds = System.currentTimeMillis() / 1000;
    String prefix             = username + ":"  + currentTimeSeconds;
    String output             = Hex.encodeHexString(Util.truncate(getHmac(key, prefix.getBytes(), mac), 10));
    String token              = prefix + ":" + output;

    return new ExternalServiceCredentials(username, token);
  }


  public boolean isValid(String token, String number, long currentTimeMillis) {
    String[] parts = token.split(":");
    Mac      mac   = getMacInstance();

    if (parts.length != 3) {
      return false;
    }

    if (!getUserId(number, mac, usernameDerivation).equals(parts[0])) {
      return false;
    }

    if (!isValidTime(parts[1], currentTimeMillis)) {
      return false;
    }

    return isValidSignature(parts[0] + ":" + parts[1], parts[2], mac);
  }

  private String getUserId(String number, Mac mac, boolean usernameDerivation) {
    if (usernameDerivation) return Hex.encodeHexString(Util.truncate(getHmac(userIdKey, number.getBytes(), mac), 10));
    else                    return number;
  }

  private boolean isValidTime(String timeString, long currentTimeMillis) {
    try {
      long tokenTime = Long.parseLong(timeString);
      long ourTime   = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);

      return TimeUnit.SECONDS.toHours(Math.abs(ourTime - tokenTime)) < 24;
    } catch (NumberFormatException e) {
      logger.warn("Number Format", e);
      return false;
    }
  }

  private boolean isValidSignature(String prefix, String suffix, Mac mac) {
    try {
      byte[] ourSuffix   = Util.truncate(getHmac(key, prefix.getBytes(), mac), 10);
      byte[] theirSuffix = Hex.decodeHex(suffix.toCharArray());

      return MessageDigest.isEqual(ourSuffix, theirSuffix);
    } catch (DecoderException e) {
      logger.warn("DirectoryCredentials", e);
      return false;
    }
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
