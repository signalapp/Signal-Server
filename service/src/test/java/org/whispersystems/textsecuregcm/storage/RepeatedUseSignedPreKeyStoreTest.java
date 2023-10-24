/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;

abstract class RepeatedUseSignedPreKeyStoreTest<K extends SignedPreKey<?>> {

  protected abstract RepeatedUseSignedPreKeyStore<K> getKeyStore();

  protected abstract K generateSignedPreKey();

  @Test
  void storeFind() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    assertEquals(Optional.empty(), keys.find(UUID.randomUUID(), Device.PRIMARY_ID).join());

    {
      final UUID identifier = UUID.randomUUID();
      final byte deviceId = 1;
      final K signedPreKey = generateSignedPreKey();

      assertDoesNotThrow(() -> keys.store(identifier, deviceId, signedPreKey).join());
      assertEquals(Optional.of(signedPreKey), keys.find(identifier, deviceId).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final byte deviceId2 = 2;
      final Map<Byte, K> signedPreKeys = Map.of(
          Device.PRIMARY_ID, generateSignedPreKey(),
          deviceId2, generateSignedPreKey()
      );

      assertDoesNotThrow(() -> keys.store(identifier, signedPreKeys).join());
      assertEquals(Optional.of(signedPreKeys.get(Device.PRIMARY_ID)), keys.find(identifier, Device.PRIMARY_ID).join());
      assertEquals(Optional.of(signedPreKeys.get(deviceId2)), keys.find(identifier, deviceId2).join());
    }
  }

  @Test
  void delete() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    assertDoesNotThrow(() -> keys.delete(UUID.randomUUID()).join());

    final byte deviceId2 = 2;
    {
      final UUID identifier = UUID.randomUUID();
      final Map<Byte, K> signedPreKeys = Map.of(
          Device.PRIMARY_ID, generateSignedPreKey(),
          deviceId2, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier, Device.PRIMARY_ID).join();

      assertEquals(Optional.empty(), keys.find(identifier, Device.PRIMARY_ID).join());
      assertEquals(Optional.of(signedPreKeys.get(deviceId2)), keys.find(identifier, deviceId2).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Byte, K> signedPreKeys = Map.of(
          Device.PRIMARY_ID, generateSignedPreKey(),
          deviceId2, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier).join();

      assertEquals(Optional.empty(), keys.find(identifier, Device.PRIMARY_ID).join());
      assertEquals(Optional.empty(), keys.find(identifier, deviceId2).join());
    }
  }
}
