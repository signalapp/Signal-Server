/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/// A versionstamp/UUID cipher encrypts and encodes FoundationDB [Versionstamp] instances as version 8 UUIDs and decodes
/// and decrypts UUIDs as `Versionstamps`.
///
/// @implNote This cipher uses the NIST FF1 format-preserving encryption algorithm to encrypt `Versionstamps`. It uses
/// a global, static AES key (although message GUIDs encode a key ID to facilitate key rotation should the need arise)
/// and uses the receiving device's ID and account identifier as a "tweak."
///
/// The layout of returned UUIDs is as follows:
///
/// ```
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                        versionstamp_a                         |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |        versionstamp_a         |  v_U  |  v_F  |   reserved    |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |var|  key_id   |   reserved    |        versionstamp_b         |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                        versionstamp_b                         |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
///
/// Where:
///
/// - `versionstamp_a` is the first 48 bits of a versionstamp encrypted with NIST FF1
/// - `versionstamp_b` is the latter 48 bits of a versionstamp encrypted with NIST FF1
/// - `key_id` is a 6-bit identifier for the key used to encrypt the versionstamp (initially 0b000001)
/// - `v_F` is 4-bit version of our application-specific v8 UUID format (initially 0b0001)
/// - `v_U` is the 4-bit version for a v8 UUID (i.e. 0b1000)
/// - `var` is the 2-bit variant for a v8 UUID (i.e. 0b10)
/// - `reserved` bits have no semantic meaning in the current version of the format and are reserved for future use
///
/// @see <a href="https://csrc.nist.gov/pubs/sp/800/38/g/r1/2pd">Recommendation for Block Cipher Modes of Operation: Methods for Format-Preserving Encryption</a>
/// @see <a href="https://apple.github.io/foundationdb/data-modeling.html#versionstamps">FoundationDB - Data Modeling - Versionstamps</a>
/// @see <a href="https://www.rfc-editor.org/rfc/rfc9562.html#name-uuid-version-8">RFC 9562 Universally Unique IDentifiers (UUIDs), Section 5.8: UUID Version 8</a>
public class VersionstampUUIDCipher {

  private final byte keyId;
  private final SecretKey key;

  @VisibleForTesting
  static final byte FORMAT_VERSION = 0x01;

  private static final int VERSIONSTAMP_LENGTH = 12; // bytes
  private static final int TWEAK_LENGTH = 17; // bytes
  private static final byte ROUNDS = 10; // from FF1 specification

  private static final IvParameterSpec IV_PARAMETERS = new IvParameterSpec(new byte[] {
      0x01, // static value from FF1 specification
      0x02, // static value from FF1 specification
      0x01, // static value from FF1 specification
      0x00, 0x00, 0x02, // radix
      ROUNDS,
      (VERSIONSTAMP_LENGTH * 8) / 2,
      0x00, 0x00, 0x00, VERSIONSTAMP_LENGTH * 8, // for radix = 2, plaintext/ciphertext length in bits
      0x00, 0x00, 0x00, TWEAK_LENGTH, // tweak length in bytes (not bits)
  });

  private static final long LEAST_SIGNIFICANT_SIX_BYTES_MASK = 0x0000_ffff_ffff_ffffL;
  private static final byte FORMAT_VERSION_MASK = 0x0f;
  private static final byte KEY_ID_MASK = 0x3f;

  static {
    //noinspection ConstantValue
    assert (FORMAT_VERSION & FORMAT_VERSION_MASK) == FORMAT_VERSION;
  }

  /// Constructs a new cipher with the given AES key.
  ///
  /// @param keyId an identifier for the given AES encryption key; must be between 0 and 63, inclusive
  /// @param keyBytes the raw bytes of an AES encryption key
  ///
  /// @throws IllegalArgumentException if the given `keyBytes` could not be used as an AES key
  public VersionstampUUIDCipher(final int keyId, final byte[] keyBytes) {
    if (keyId != (keyId & KEY_ID_MASK)) {
      throw new IllegalArgumentException("Key ID must be between 0 and 63, inclusive");
    }

    this.keyId = (byte) (keyId & KEY_ID_MASK);
    this.key = new SecretKeySpec(keyBytes, "AES");

    // Fail fast on invalid parameters
    try {
      getCipher().init(Cipher.ENCRYPT_MODE, key, IV_PARAMETERS);
    } catch (final InvalidAlgorithmParameterException e) {
      throw new AssertionError("Known, static IV invalid", e);
    } catch (final InvalidKeyException e) {
      throw new IllegalArgumentException("Key bytes could not be used as an AES key", e);
    }
  }

   /// Encrypts a `Versionstamp` and encodes it as version 8 UUID the given account identifier.
   ///
   /// @implNote the account identifier is used as a cryptographic "tweak" as described in the FF1 specification
   ///
   /// @param versionstamp the versionstamp to encrypt
   /// @param accountIdentifier the account identifier for which to encrypt the versionstamp
   /// @param deviceId the ID of the device within the given account for which to encrypt the versionstamp
   ///
   /// @return a version 8 UUID that encodes the encrypted versionstamp
   ///
   /// @see #decryptVersionstamp(UUID, UUID, byte)
  public UUID encryptVersionstamp(final Versionstamp versionstamp, final UUID accountIdentifier, final byte deviceId) {
    final Cipher cipher = getCipher();
    final byte[] buffer = new byte[16];

    long a, b;
    {
      final long[] longs = byteArrayToLongs(versionstamp.getBytes());

      a = longs[0];
      b = longs[1];
    }

    for (byte round = 0; round < ROUNDS; round++) {
      final long c = (a + doRound(cipher, accountIdentifier, deviceId, round, b, buffer)) & LEAST_SIGNIFICANT_SIX_BYTES_MASK;

      a = b;
      b = c;
    }

    final long uuidVersionAndFormatVersion = (0x80L | (FORMAT_VERSION & FORMAT_VERSION_MASK)) << 8;
    final long uuidVariantAndKeyId = (0x80L | (keyId)) << 56;

    return new UUID(a << 16 | uuidVersionAndFormatVersion, b | uuidVariantAndKeyId);
  }

  /// Decrypts a versionstamp encoded as a version 8 UUID for the given account identifier and device ID.
  ///
  /// @implNote the account identifier and device ID are used as a cryptographic "tweak" as described in the FF1
  /// specification
  ///
  /// @param encryptedVersionstamp the encrypted versionstamp to decrypt
  /// @param accountIdentifier the account identifier for which to decrypt the versionstamp
  /// @param deviceId the ID of the device within the account for which to decrypt the versionstamp
  ///
  /// @return the versionstamp from the given ciphertext and for the given account identifier and device ID
  public Versionstamp decryptVersionstamp(final UUID encryptedVersionstamp, final UUID accountIdentifier, final byte deviceId) {
    if (encryptedVersionstamp.version() != 8) {
      throw new IllegalArgumentException("Unexpected UUID version");
    }

    if (getFormatVersion(encryptedVersionstamp) != FORMAT_VERSION) {
      throw new IllegalArgumentException("Unexpected format version");
    }

    if (getKeyId(encryptedVersionstamp) != keyId) {
      throw new IllegalArgumentException("Unexpected key ID");
    }

    final Cipher cipher = getCipher();
    final byte[] buffer = new byte[16];

    long a = encryptedVersionstamp.getMostSignificantBits() >>> 16;
    long b = encryptedVersionstamp.getLeastSignificantBits() & LEAST_SIGNIFICANT_SIX_BYTES_MASK;

    for (byte round = ROUNDS - 1; round >= 0; round--) {
      final long c = b;
      b = a;

      a = (c - doRound(cipher, accountIdentifier, deviceId, round, b, buffer)) & LEAST_SIGNIFICANT_SIX_BYTES_MASK;
    }

    return Versionstamp.fromBytes(longsToByteArray(a, b));
  }

  @VisibleForTesting
  static byte getFormatVersion(final UUID encryptedVersionstamp) {
    if (encryptedVersionstamp.version() != 8) {
      throw new IllegalArgumentException("Unexpected UUID version");
    }

    return (byte) ((encryptedVersionstamp.getMostSignificantBits() & 0x0000_0000_0000_0f00L) >>> 8);
  }

  @VisibleForTesting
  static byte getKeyId(final UUID encryptedVersionstamp) {
    if (encryptedVersionstamp.version() != 8) {
      throw new IllegalArgumentException("Unexpected UUID version");
    }

    return (byte) ((encryptedVersionstamp.getLeastSignificantBits() & 0x3f00_0000_0000_0000L) >>> 56);
  }

  private long doRound(final Cipher cipher,
      final UUID accountIdentifier,
      final byte deviceId,
      final byte round,
      final long b,
      final byte[] bufferArray) {

    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, IV_PARAMETERS);
    } catch (final InvalidKeyException e) {
      throw new AssertionError("Previously-valid key now invalid", e);
    } catch (final InvalidAlgorithmParameterException e) {
      throw new AssertionError("Previously-valid IV now invalid", e);
    }

    assert (b & ~LEAST_SIGNIFICANT_SIX_BYTES_MASK) == 0;

    final ByteBuffer byteBuffer = ByteBuffer.wrap(bufferArray);

    // Write the first part of cryptographic "tweak" into the buffer. We have one more byte (the device ID) that will
    // "spill" over into the next block, but that's okay! That extra byte falls into the "padding zone" of the next
    // block, and doesn't increase the overall block count per round.
    byteBuffer
        .putLong(0, accountIdentifier.getMostSignificantBits())
        .putLong(8, accountIdentifier.getLeastSignificantBits());

    try {
      // We can overwrite the input buffer here to avoid allocating an additional 16-byte buffer that we'd just
      // discard immediately
      cipher.update(bufferArray, 0, bufferArray.length, bufferArray);

      // We want to wind up with the device ID (the last byte of the "tweak"), followed by 8 bytes of zeroes, followed
      // by a one-byte round counter, finally followed by the lowest 6 bytes of `b`. The highest two bytes of `b` are
      // already 0 (as asserted above). We write `b` first so we can overwrite one of its high bytes with the round
      // counter. Note that we can overwrite the AES-CBC output block that's currently in the buffer; it's just an
      // intermediate value that we don't actually need to return (even though it's used internally by AES-CBC).
      byteBuffer
          .putLong(0, 0L)
          .put(0, deviceId)
          .putLong(8, b)
          .put(9, round);

      // Again, overwrite the input buffer to avoid an extra buffer allocation…
      cipher.update(bufferArray, 0, bufferArray.length, bufferArray);

      // …and then overwrite the buffer once more with the cipher's finished output, which is the only AES-CBC output we
      // actually use directly.
      final int bytesWritten = cipher.doFinal(bufferArray, 0);
      assert bytesWritten == 16;

      return byteBuffer.getLong(0) & LEAST_SIGNIFICANT_SIX_BYTES_MASK;
    } catch (final IllegalBlockSizeException | BadPaddingException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support AES/CBC/PKCS5Padding", e);
    } catch (final ShortBufferException e) {
      throw new AssertionError("Buffer with known length of 16 too short", e);
    }
  }

  /// Returns a `Cipher` instance suitable for use in the core NIST/FF1 loop.
  ///
  /// @return a `Cipher` instance suitable for use in the core NIST/FF1 loop
  private static Cipher getCipher() {
    try {
      return Cipher.getInstance("AES/CBC/PKCS5Padding");
    } catch (final NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support AES/CBC/PKCS5Padding", e);
    }
  }

  /// Encode a 96-bit value, supplied as a `byte[12]`, as a pair of right-aligned `long`s. The first `long` contains the
  /// first six bytes of the input and the second `long` contains the next six bytes, both interpreted in big-endian
  /// order. That is, given the `byte` array `{ 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc}`,
  /// this function will return the `long` array `{ 0x0000_1122_3344_5566, 0x0000_7788_99aa_bbcc }`.
  ///
  /// @param bytes the bytes to encode as a pair of `long`s
  ///
  /// @return a pair of `long` instances that encode the original value
  ///
  /// @see #longsToByteArray(long, long)
  @VisibleForTesting
  static long[] byteArrayToLongs(final byte[] bytes) {
    assert bytes.length == VERSIONSTAMP_LENGTH;

    final ByteBuffer buffer = ByteBuffer.wrap(bytes);

    return new long[] {
        buffer.getLong(0) >>> 16,
        buffer.getLong(4) & LEAST_SIGNIFICANT_SIX_BYTES_MASK
    };
  }

  /// Packs the least-significant 48 bits of the given `long`s into a single 96-bit array. The least significant 48 bits
  /// of the first `long` become the first 48 bits of the returned array, and the least significant bits of the second
  /// `long` become the second 48 bits of the returned array, both encoded in big-endian order.
  ///
  /// @param mostSignificantBits a `long` carrying the most significant 48 bits of the combined value
  /// @param leastSignificantBits a `long` carrying the least significant 48 bits of the combined value
  ///
  /// @return a byte array containing the combined 96-bit value from the two given `long`s
  ///
  /// @see #byteArrayToLongs(byte[])
  @VisibleForTesting
  static byte[] longsToByteArray(final long mostSignificantBits, final long leastSignificantBits) {
    final byte[] versionstamp = new byte[VERSIONSTAMP_LENGTH];

    versionstamp[0] = (byte) (mostSignificantBits >> 40 & 0xff);
    versionstamp[1] = (byte) (mostSignificantBits >> 32 & 0xff);
    versionstamp[2] = (byte) (mostSignificantBits >> 24 & 0xff);
    versionstamp[3] = (byte) (mostSignificantBits >> 16 & 0xff);
    versionstamp[4] = (byte) (mostSignificantBits >>  8 & 0xff);
    versionstamp[5] = (byte) (mostSignificantBits       & 0xff);

    versionstamp[6]  = (byte) (leastSignificantBits >> 40 & 0xff);
    versionstamp[7]  = (byte) (leastSignificantBits >> 32 & 0xff);
    versionstamp[8]  = (byte) (leastSignificantBits >> 24 & 0xff);
    versionstamp[9]  = (byte) (leastSignificantBits >> 16 & 0xff);
    versionstamp[10] = (byte) (leastSignificantBits >>  8 & 0xff);
    versionstamp[11] = (byte) (leastSignificantBits       & 0xff);

    return versionstamp;
  }
}
