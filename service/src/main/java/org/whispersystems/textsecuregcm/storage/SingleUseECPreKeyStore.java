/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;

public class SingleUseECPreKeyStore extends SingleUsePreKeyStore<PreKey> {

  private static final Counter INVALID_KEY_COUNTER =
      Metrics.counter(MetricsUtil.name(SingleUseECPreKeyStore.class, "invalidKey"));

  protected SingleUseECPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID identifier, final long deviceId, final PreKey preKey) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, preKey.getKeyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(preKey.getPublicKey()));
  }

  @Override
  protected PreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    final byte[] publicKey = extractByteArray(item.get(ATTR_PUBLIC_KEY));

    try {
      new ECPublicKey(publicKey);
    } catch (final InvalidKeyException e) {
      INVALID_KEY_COUNTER.increment();
    }

    return new PreKey(keyId, publicKey);
  }
}
