package org.whispersystems.textsecuregcm.backup;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public record MediaEncryptionParameters(
    SecretKeySpec aesEncryptionKey,
    SecretKeySpec hmacSHA256Key,
    IvParameterSpec iv) {

  public MediaEncryptionParameters(byte[] encryptionKey, byte[] macKey, byte[] iv) {
    this(
        new SecretKeySpec(encryptionKey, "AES"),
        new SecretKeySpec(macKey, "HmacSHA256"),
        new IvParameterSpec(iv));
  }
}
