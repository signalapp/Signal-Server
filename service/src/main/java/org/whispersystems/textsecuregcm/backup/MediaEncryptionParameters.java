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

  public int outputSize(final int inputSize) {
    // AES-256 has 16-byte block size, and always adds a block if the plaintext is a multiple of the block size
    final int numBlocks = (inputSize + 16) / 16;
    // IV + AES-256 encrypted data + HmacSHA256
    return this.iv().getIV().length + (numBlocks * 16) + 32;
  }
}
