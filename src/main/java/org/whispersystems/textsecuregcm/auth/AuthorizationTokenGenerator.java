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

public class AuthorizationTokenGenerator {

  private final Logger logger = LoggerFactory.getLogger(AuthorizationTokenGenerator.class);

  private final byte[] key;

  public AuthorizationTokenGenerator(byte[] key) {
    this.key = key;
  }

  public AuthorizationToken generateFor(String number) {
    try {
      Mac    mac                = Mac.getInstance("HmacSHA256");
      long   currentTimeSeconds = System.currentTimeMillis() / 1000;
      String prefix             = number + ":"  + currentTimeSeconds;

      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      String output = Hex.encodeHexString(Util.truncate(mac.doFinal(prefix.getBytes()), 10));
      String token  = prefix + ":" + output;

      return new AuthorizationToken(token);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }


  public boolean isValid(String token, String number, long currentTimeMillis) {
    String[] parts = token.split(":");

    if (parts.length != 3) {
      return false;
    }

    if (!number.equals(parts[0])) {
      return false;
    }

    if (!isValidTime(parts[1], currentTimeMillis)) {
      return false;
    }

    return isValidSignature(parts[0] + ":" + parts[1], parts[2]);
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

  private boolean isValidSignature(String prefix, String suffix) {
    try {
      Mac hmac = Mac.getInstance("HmacSHA256");
      hmac.init(new SecretKeySpec(key, "HmacSHA256"));

      byte[] ourSuffix   = Util.truncate(hmac.doFinal(prefix.getBytes()), 10);
      byte[] theirSuffix = Hex.decodeHex(suffix.toCharArray());

      return MessageDigest.isEqual(ourSuffix, theirSuffix);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    } catch (DecoderException e) {
      logger.warn("Authorizationtoken", e);
      return false;
    }
  }

}
