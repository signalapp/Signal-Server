/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import org.signal.libsignal.protocol.IdentityKey;
import java.nio.ByteBuffer;

public class EncryptDeviceCreationTimestampUtil {
  @VisibleForTesting
  final static String ENCRYPTION_INFO = "deviceCreatedAt";

  /**
   * Encrypts the provided timestamp with the ACI identity key using the
   * Hybrid Public Key Encryption scheme as defined in (<a href="https://www.rfc-editor.org/rfc/rfc9180.html">RFC 9180</a>).
   *
   * @param createdAt The timestamp in milliseconds since epoch when a given device was linked to the account.
   * @param aciIdentityKey The ACI identity key associated with the account.
   * @param deviceId The ID of the given device.
   * @param registrationId The registration ID of the given device.
   *
   * @return The timestamp ciphertext
   */
  public static byte[] encrypt(
      final long createdAt,
      final IdentityKey aciIdentityKey,
      final byte deviceId,
      final int registrationId) {
    final ByteBuffer timestampBytes = ByteBuffer.allocate(8);
    timestampBytes.putLong(createdAt);

    // "Associated data" is metadata that ties the ciphertext to a specific context to prevent an adversary from
    // swapping the ciphertext of two devices on the same account.
    final ByteBuffer associatedData = ByteBuffer.allocate(5);
    associatedData.put(deviceId);
    associatedData.putInt(registrationId);

    return aciIdentityKey.getPublicKey().seal(timestampBytes.array(), ENCRYPTION_INFO, associatedData.array());
  }
}
