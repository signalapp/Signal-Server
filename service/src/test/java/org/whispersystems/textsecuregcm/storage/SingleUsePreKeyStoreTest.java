/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.entities.PreKey;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

abstract class SingleUsePreKeyStoreTest<K extends PreKey<?>> {

  private static final int KEY_COUNT = 100;

  protected abstract SingleUsePreKeyStore<K> getPreKeyStore();

  protected abstract K generatePreKey(final long keyId);

  @Test
  void storeTake() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final long deviceId = 1;

    assertEquals(Optional.empty(), preKeyStore.take(accountIdentifier, deviceId).join());

    final List<K> preKeys = new ArrayList<>(KEY_COUNT);

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeys.add(generatePreKey(i));
    }

    assertDoesNotThrow(() -> preKeyStore.store(accountIdentifier, deviceId, preKeys).join());

    assertEquals(Optional.of(preKeys.get(0)), preKeyStore.take(accountIdentifier, deviceId).join());
    assertEquals(Optional.of(preKeys.get(1)), preKeyStore.take(accountIdentifier, deviceId).join());
  }

  @Test
  void getCount() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final long deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());

    final List<K> preKeys = new ArrayList<>(KEY_COUNT);

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeys.add(generatePreKey(i));
    }

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();

    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, deviceId).join());
  }

  @Test
  void deleteSingleDevice() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final long deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    final List<K> preKeys = new ArrayList<>(KEY_COUNT);

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeys.add(generatePreKey(i));
    }

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, deviceId + 1, preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier, deviceId).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(KEY_COUNT, preKeyStore.getCount(accountIdentifier, deviceId + 1).join());
  }

  @Test
  void deleteAllDevices() {
    final SingleUsePreKeyStore<K> preKeyStore = getPreKeyStore();

    final UUID accountIdentifier = UUID.randomUUID();
    final long deviceId = 1;

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    final List<K> preKeys = new ArrayList<>(KEY_COUNT);

    for (int i = 0; i < KEY_COUNT; i++) {
      preKeys.add(generatePreKey(i));
    }

    preKeyStore.store(accountIdentifier, deviceId, preKeys).join();
    preKeyStore.store(accountIdentifier, deviceId + 1, preKeys).join();

    assertDoesNotThrow(() -> preKeyStore.delete(accountIdentifier).join());

    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(0, preKeyStore.getCount(accountIdentifier, deviceId + 1).join());
  }
}
