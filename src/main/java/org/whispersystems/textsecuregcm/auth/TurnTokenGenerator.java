package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.configuration.TurnConfiguration;
import org.whispersystems.textsecuregcm.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TurnTokenGenerator {

  private final byte[]       key;
  private final List<String> urls;

  public TurnTokenGenerator(TurnConfiguration configuration) {
    this.key  = configuration.getSecret().getBytes();
    this.urls = configuration.getUris();
  }

  public TurnToken generate() {
    try {
      Mac    mac                = Mac.getInstance("HmacSHA1");
      long   validUntilSeconds  = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) / 1000;
      long   user               = Math.abs(new SecureRandom().nextInt());
      String userTime           = validUntilSeconds + ":"  + user;

      mac.init(new SecretKeySpec(key, "HmacSHA1"));
      String password = Base64.encodeBytes(mac.doFinal(userTime.getBytes()));

      return new TurnToken(userTime, password, urls);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }
}
