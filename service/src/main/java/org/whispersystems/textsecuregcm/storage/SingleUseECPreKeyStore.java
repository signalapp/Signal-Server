/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class SingleUseECPreKeyStore extends SingleUsePreKeyStore<ECPreKey> {
  private static final String PARSE_BYTE_ARRAY_COUNTER_NAME = name(SingleUseECPreKeyStore.class, "parseByteArray");

  public SingleUseECPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID identifier,
      final byte deviceId,
      final ECPreKey preKey,
      final int remainingKeys) {

    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, preKey.keyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(preKey.serializedPublicKey()),
        ATTR_REMAINING_KEYS, AttributeValues.fromInt(remainingKeys));
  }

  @Override
  protected ECPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    final byte[] publicKey = AttributeValues.extractByteArray(item.get(ATTR_PUBLIC_KEY), PARSE_BYTE_ARRAY_COUNTER_NAME);

    try {
      return new ECPreKey(keyId, new ECPublicKey(publicKey));
    } catch (final InvalidKeyException e) {
      // This should never happen since we're serializing keys directly from `ECPublicKey` instances on the way in
      throw new IllegalArgumentException(e);
    }
  }
}
