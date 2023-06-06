/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;

public class SingleUseKEMPreKeyStore extends SingleUsePreKeyStore<SignedPreKey> {

  protected SingleUseKEMPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID identifier, final long deviceId, final SignedPreKey signedPreKey) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, signedPreKey.getKeyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(signedPreKey.getPublicKey()),
        ATTR_SIGNATURE, AttributeValues.fromByteArray(signedPreKey.getSignature()));
  }

  @Override
  protected SignedPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    final byte[] publicKey = extractByteArray(item.get(ATTR_PUBLIC_KEY));
    final byte[] signature = extractByteArray(item.get(ATTR_SIGNATURE));

    return new SignedPreKey(keyId, publicKey, signature);
  }
}
