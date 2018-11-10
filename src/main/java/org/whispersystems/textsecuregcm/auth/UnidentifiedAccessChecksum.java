package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class UnidentifiedAccessChecksum {

  public static String generateFor(Optional<byte[]> unidentifiedAccessKey) {
    try {
      if (!unidentifiedAccessKey.isPresent()|| unidentifiedAccessKey.get().length != 16) return null;

      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(unidentifiedAccessKey.get(), "HmacSHA256"));

      return Base64.encodeBytes(mac.doFinal(new byte[32]));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

}
