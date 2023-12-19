/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class RepeatedUseKEMSignedPreKeyStoreTest extends RepeatedUseSignedPreKeyStoreTest<KEMSignedPreKey> {

  private RepeatedUseKEMSignedPreKeyStore keyStore;

  private int currentKeyId = 1;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @BeforeEach
  void setUp() {
    keyStore = new RepeatedUseKEMSignedPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName());
  }

  @Override
  protected RepeatedUseSignedPreKeyStore<KEMSignedPreKey> getKeyStore() {
    return keyStore;
  }

  @Override
  protected DynamoDbClient getDynamoDbClient() {
    return DYNAMO_DB_EXTENSION.getDynamoDbClient();
  }

  @Override
  protected KEMSignedPreKey generateSignedPreKey() {
    return KeysHelper.signedKEMPreKey(currentKeyId++, IDENTITY_KEY_PAIR);
  }
}
