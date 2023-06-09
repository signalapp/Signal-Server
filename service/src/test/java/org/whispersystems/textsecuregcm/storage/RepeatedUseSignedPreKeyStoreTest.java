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

    assertEquals(Optional.empty(), keys.find(UUID.randomUUID(), 1).join());

    {
      final UUID identifier = UUID.randomUUID();
      final long deviceId = 1;
      final K signedPreKey = generateSignedPreKey();

      assertDoesNotThrow(() -> keys.store(identifier, deviceId, signedPreKey).join());
      assertEquals(Optional.of(signedPreKey), keys.find(identifier, deviceId).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, K> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      assertDoesNotThrow(() -> keys.store(identifier, signedPreKeys).join());
      assertEquals(Optional.of(signedPreKeys.get(1L)), keys.find(identifier, 1).join());
      assertEquals(Optional.of(signedPreKeys.get(2L)), keys.find(identifier, 2).join());
    }
  }

  @Test
  void delete() {
    final RepeatedUseSignedPreKeyStore<K> keys = getKeyStore();

    assertDoesNotThrow(() -> keys.delete(UUID.randomUUID()).join());

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, K> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier, 1).join();

      assertEquals(Optional.empty(), keys.find(identifier, 1).join());
      assertEquals(Optional.of(signedPreKeys.get(2L)), keys.find(identifier, 2).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, K> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier).join();

      assertEquals(Optional.empty(), keys.find(identifier, 1).join());
      assertEquals(Optional.empty(), keys.find(identifier, 2).join());
    }
  }
}
