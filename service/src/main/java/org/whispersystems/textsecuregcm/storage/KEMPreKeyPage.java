/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.List;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;

class KEMPreKeyPage {

  static final byte FORMAT = 1;

  // Serialized pages start with a 4 byte magic constant, followed by 3 bytes of 0s and then the format byte
  static final int HEADER_MAGIC = 0xC21C6DB8;
  static final int HEADER_SIZE = 8;
  // Serialize bigendian to produce the serialized page header
  private static final long HEADER = ((long) HEADER_MAGIC) << 32L | (long) FORMAT;

  // The length of libsignal's serialized KEM public key, which is a single-byte version followed by the public key
  private static final int SERIALIZED_PUBKEY_LENGTH = 1569;
  private static final int SERIALIZED_SIGNATURE_LENGTH = 64;
  private static final int KEY_ID_LENGTH = Long.BYTES;

  // The internal prefix byte libsignal uses to indicate a key is of type KEMKeyType.KYBER_1024. Currently, this
  // is the only type of key allowed to be written to a prekey page
  private static final byte KEM_KEY_TYPE_KYBER_1024 = 0x08;

  @VisibleForTesting
  static final int SERIALIZED_PREKEY_LENGTH = KEY_ID_LENGTH + SERIALIZED_PUBKEY_LENGTH + SERIALIZED_SIGNATURE_LENGTH;

  private KEMPreKeyPage() {}

  /**
   * Serialize the list of preKeys into a single buffer
   *
   * @param format the format to serialize as. Currently, the only valid format is {@link KEMPreKeyPage#FORMAT}
   * @param preKeys the preKeys to serialize
   * @return The serialized buffer and a format to store alongside the buffer
   */
  static ByteBuffer serialize(final byte format, final List<KEMSignedPreKey> preKeys) {
    if (format != FORMAT) {
      throw new IllegalArgumentException("Unknown format: " + format + ", must be " + FORMAT);
    }

    if (preKeys.isEmpty()) {
      throw new IllegalArgumentException("PreKeys cannot be empty");
    }
    final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + SERIALIZED_PREKEY_LENGTH * preKeys.size());
    buffer.putLong(HEADER);
    for (KEMSignedPreKey preKey : preKeys) {

      buffer.putLong(preKey.keyId());

      final byte[] publicKeyBytes = preKey.serializedPublicKey();
      if (publicKeyBytes[0] != KEM_KEY_TYPE_KYBER_1024) {
        // 0x08 is libsignal's current KEM key format. If some future version of libsignal supports additional KEM
        // keys, we'll have to roll out read support before rolling out write support. Otherwise, we may write keys
        // to storage that are not readable by other chat instances.
        throw new IllegalArgumentException("Format 1 only supports " + KEM_KEY_TYPE_KYBER_1024 + " public keys");
      }
      if (publicKeyBytes.length != SERIALIZED_PUBKEY_LENGTH) {
        throw new IllegalArgumentException("Unexpected public key length " + publicKeyBytes.length);
      }
      buffer.put(publicKeyBytes);

      if (preKey.signature().length != SERIALIZED_SIGNATURE_LENGTH) {
        throw new IllegalArgumentException("prekey signature length must be " + SERIALIZED_SIGNATURE_LENGTH);
      }
      buffer.put(preKey.signature());
    }
    buffer.flip();
    return buffer;
  }

  /**
   * Deserialize a single {@link KEMSignedPreKey}
   *
   * @param format The format of the page this buffer is from
   * @param buffer The key to deserialize. The position of the buffer should be the start of the key, and the limit of
   *               the buffer should be the end of the key. After a successful deserialization the position of the
   *               buffer will be the limit
   * @return The deserialized key
   * @throws InvalidKeyException
   */
  static KEMSignedPreKey deserializeKey(int format, ByteBuffer buffer) throws InvalidKeyException {
    if (format != FORMAT) {
      throw new IllegalArgumentException("Unknown prekey page format " + format);
    }
    if (buffer.remaining() != SERIALIZED_PREKEY_LENGTH) {
      throw new IllegalArgumentException("PreKeys must be length " + SERIALIZED_PREKEY_LENGTH);
    }
    final long keyId = buffer.getLong();

    final byte[] publicKeyBytes = new byte[SERIALIZED_PUBKEY_LENGTH];
    buffer.get(publicKeyBytes);
    final KEMPublicKey kemPublicKey = new KEMPublicKey(publicKeyBytes);

    final byte[] signature = new byte[SERIALIZED_SIGNATURE_LENGTH];
    buffer.get(signature);
    return new KEMSignedPreKey(keyId, kemPublicKey, signature);
  }

  /**
   * The location of a specific key within a serialized page
   */
  record KeyLocation(int start, int length) {

    int getStartInclusive() {
      return start;
    }

    int getEndInclusive() {
      return start + length - 1;
    }
  }

  /**
   * Get the location of the key at the provided index within a page
   *
   * @param format The format of the page
   * @param index  The index of the key to retrieve
   * @return An {@link KeyLocation} indicating where within the page the key is
   */
  static KeyLocation keyLocation(final int format, final int index) {
    if (format != FORMAT) {
      throw new IllegalArgumentException("unknown format " + format);
    }
    final int startOffset = HEADER_SIZE + (index * SERIALIZED_PREKEY_LENGTH);
    return new KeyLocation(startOffset, SERIALIZED_PREKEY_LENGTH);
  }
}
