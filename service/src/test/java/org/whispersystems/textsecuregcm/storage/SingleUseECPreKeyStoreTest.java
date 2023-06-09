/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.whispersystems.textsecuregcm.entities.ECPreKey;

class SingleUseECPreKeyStoreTest extends SingleUsePreKeyStoreTest<ECPreKey> {

  private SingleUseECPreKeyStore preKeyStore;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.EC_KEYS);

  @BeforeEach
  void setUp() {
    preKeyStore = new SingleUseECPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.EC_KEYS.tableName());
  }

  @Override
  protected SingleUsePreKeyStore<ECPreKey> getPreKeyStore() {
    return preKeyStore;
  }

  @Override
  protected ECPreKey generatePreKey(final long keyId) {
    return new ECPreKey(keyId, Curve.generateKeyPair().getPublicKey());
  }
}
