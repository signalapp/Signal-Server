package org.whispersystems.textsecuregcm.backup;

import javax.crypto.spec.SecretKeySpec;

public record MediaEncryptionParameters(
    SecretKeySpec aesEncryptionKey,
    SecretKeySpec hmacSHA256Key) {

  public MediaEncryptionParameters(byte[] encryptionKey, byte[] macKey) {
    this(
        new SecretKeySpec(encryptionKey, "AES"),
        new SecretKeySpec(macKey, "HmacSHA256"));
  }
}
