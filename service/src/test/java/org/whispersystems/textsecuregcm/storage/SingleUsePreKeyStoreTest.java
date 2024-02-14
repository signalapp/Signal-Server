/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.PreKey;

abstract class SingleUsePreKeyStoreTest<K extends PreKey<?>> {

  private static final int KEY_COUNT = 100;

  protected abstract SingleUsePreKeyStore<K> getPreKeyStore();

  protected abstract K generatePreKey(final long keyId);

  protected abstract void clearKeyCountAttributes();

  @Test
  void storeTake() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(Optional.empty(), preKeyStore.take(accountIdentifier, deviceId).join());

    final List<K> sortedPreKeys;
    {
      final List<K> preKeys = generateRandomPreKeys();
      assertDoesNotThrow(() -> preKeyStore.store(accountIdentifier, deviceId, preKeys).join());

      sortedPreKeys = new ArrayList<>(preKeys);
      sortedPreKeys.sort(Comparator.comparing(preKey -> preKey.keyId()));
    }

    assertEquals(Optional.of(sortedPreKeys.get(0)), preKeyStore.take(accountIdentifier, deviceId).join());
    assertEquals(Optional.of(sortedPreKeys.get(1)), preKeyStore.take(accountIdentifier, deviceId).join());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getCount(final boolean hasKeyCountAttribute) {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());

    final List<K> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();

    if (!hasKeyCountAttribute) {
      clearKeyCountAttributes();
    }

    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, deviceId).join());

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeyStore.take(accountIdentifier, deviceId).join();
      assertEquals(KEY_COUNT - (i + 1), preKeyStore.getCount(accountIdentifier, deviceId).join());
    }
  }

  @Test
  void peekCount() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(Optional.of(0), preKeyStore.peekCount(accountIdentifier, deviceId).join());

    final List<K> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();

    assertEquals(Optional.of(KEY_COUNT), preKeyStore.peekCount(accountIdentifier, deviceId).join());

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeyStore.take(accountIdentifier, deviceId).join();
      assertEquals(Optional.of(KEY_COUNT - (i + 1)), preKeyStore.peekCount(accountIdentifier, deviceId).join());
    }

    preKeyStore.store(accountIdentifier, deviceId, List.of(generatePreKey(KEY_COUNT + 1))).join();
    clearKeyCountAttributes();

    assertEquals(Optional.empty(), preKeyStore.peekCount(accountIdentifier, deviceId).join());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void scanCount(final boolean hasKeyCountAttribute) {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.scanCount(accountIdentifier, deviceId).join());

    final List<K> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();

    if (!hasKeyCountAttribute) {
      clearKeyCountAttributes();
    }

    assertEquals(KEY_COUNT, preKeyStore.scanCount(accountIdentifier, deviceId).join());

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeyStore.take(accountIdentifier, deviceId).join();
      assertEquals(KEY_COUNT - (i + 1), preKeyStore.scanCount(accountIdentifier, deviceId).join());
    }
  }

  @Test
  void deleteSingleDevice() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    final List<K> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());
  }

  @Test
  void deleteAllDevices() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    final List<K> preKeys = generateRandomPreKeys();

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(0, preKeyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());
  }

  private List<K> generateRandomPreKeys() {
    final Set<Integer> keyIds = new HashSet<>(KEY_COUNT);

    while (keyIds.size() < KEY_COUNT) {
      keyIds.add(Math.abs(ThreadLocalRandom.current().nextInt()));
    }

    return keyIds.stream()
        .map(this::generatePreKey)
        .toList();
  }
}
