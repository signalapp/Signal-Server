/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class RepeatedUseECSignedPreKeyStoreTest extends RepeatedUseSignedPreKeyStoreTest<ECSignedPreKey> {

  private RepeatedUseECSignedPreKeyStore keyStore;

  private int currentKeyId = 1;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS);

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @BeforeEach
  void setUp() {
    keyStore = new RepeatedUseECSignedPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName());
  }

  @Override
  protected RepeatedUseSignedPreKeyStore<ECSignedPreKey> getKeyStore() {
    return keyStore;
  }

  @Override
  protected ECSignedPreKey generateSignedPreKey() {
    return KeysHelper.signedECPreKey(currentKeyId++, IDENTITY_KEY_PAIR);
  }

  @Test
  void storeIfAbsent() {
    final UUID identifier = UUID.randomUUID();
    final long deviceIdWithExistingKey = 1;
    final long deviceIdWithoutExistingKey = deviceIdWithExistingKey + 1;

    final ECSignedPreKey originalSignedPreKey = generateSignedPreKey();

    keyStore.store(identifier, deviceIdWithExistingKey, originalSignedPreKey).join();

    assertFalse(keyStore.storeIfAbsent(identifier, deviceIdWithExistingKey, generateSignedPreKey()).join());
    assertTrue(keyStore.storeIfAbsent(identifier, deviceIdWithoutExistingKey, generateSignedPreKey()).join());

    assertEquals(Optional.of(originalSignedPreKey), keyStore.find(identifier, deviceIdWithExistingKey).join());
    assertTrue(keyStore.find(identifier, deviceIdWithoutExistingKey).join().isPresent());
  }
}
