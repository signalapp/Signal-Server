/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;
import java.util.UUID;

public class SingleUseKEMPreKeyStore extends SingleUsePreKeyStore<SignedPreKey> {

  private static final Counter INVALID_KEY_COUNTER =
      Metrics.counter(MetricsUtil.name(SingleUseKEMPreKeyStore.class, "invalidKey"));

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

    try {
      new KEMPublicKey(publicKey);
    } catch (final InvalidKeyException e) {
      INVALID_KEY_COUNTER.increment();
    }

    return new SignedPreKey(keyId, publicKey, signature);
  }
}
