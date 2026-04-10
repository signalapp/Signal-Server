/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

import com.google.common.annotations.VisibleForTesting;

/**
 * Descriptor for a single copy-and-encrypt operation
 *
 * @param sourceCdn            The cdn of the object to copy
 * @param sourceKey            The mediaId within the cdn of the object to copy
 * @param sourceLength         The length of the object to copy
 * @param encryptionParameters Encryption parameters to double encrypt the object
 * @param destinationMediaId   The mediaId of the destination object
 */
public record CopyParameters(
    int sourceCdn,
    String sourceKey,
    int sourceLength,
    MediaEncryptionParameters encryptionParameters,
    byte[] destinationMediaId) {

  /**
   * @return The size of the double-encrypted destination object after it is copied
   */
  long destinationObjectSize() {
    return destinationObjectSize(sourceLength());
  }

  /// Calculates the size of a ciphertext for a media object stored as part of a backup
  ///
  /// @param inputSize the size, in bytes, of the plaintext media object
  ///
  /// @return the size, in bytes, of the ciphertext of a media object with the given `inputSize`
  @VisibleForTesting
  static long destinationObjectSize(final int inputSize) {
    if (inputSize < 0) {
      throw new IllegalArgumentException("Size must be non-negative, but was " + inputSize);
    }

    // AES-256 has 16-byte block size, and always adds a block if the plaintext is a multiple of the block size
    final long numBlocks = ((long) inputSize + 16) / 16;

    // 16-byte IV will be generated and prepended to the ciphertext
    // IV + AES-256 encrypted data + HmacSHA256
    return 16 + (numBlocks * 16) + 32;
  }
}
