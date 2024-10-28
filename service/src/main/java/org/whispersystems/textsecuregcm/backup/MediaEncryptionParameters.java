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

  public int outputSize(final int inputSize) {
    // AES-256 has 16-byte block size, and always adds a block if the plaintext is a multiple of the block size
    final int numBlocks = (inputSize + 16) / 16;
    // 16-byte IV will be generated and prepended to the ciphertext
    // IV + AES-256 encrypted data + HmacSHA256
    return 16 + (numBlocks * 16) + 32;
  }
}
