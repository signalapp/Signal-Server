/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.whispersystems.textsecuregcm.util.EncryptDeviceCreationTimestampUtil.ENCRYPTION_INFO;

public class EncryptDeviceCreationTimestampUtilTest {
  @Test
  void encryptDecrypt() throws InvalidMessageException {
    final long createdAt = System.currentTimeMillis();
    final ECKeyPair keyPair = ECKeyPair.generate();
    final byte deviceId = 1;
    final int registrationId = 123;

    final byte[] ciphertext = EncryptDeviceCreationTimestampUtil.encrypt(createdAt, new IdentityKey(keyPair.getPublicKey()),
        deviceId, registrationId);
    final ByteBuffer associatedData = ByteBuffer.allocate(5);
    associatedData.put(deviceId);
    associatedData.putInt(registrationId);

    final byte[] decryptedData = keyPair.getPrivateKey().open(ciphertext, ENCRYPTION_INFO, associatedData.array());

    assertEquals(createdAt, ByteBuffer.wrap(decryptedData).getLong());
  }
}
