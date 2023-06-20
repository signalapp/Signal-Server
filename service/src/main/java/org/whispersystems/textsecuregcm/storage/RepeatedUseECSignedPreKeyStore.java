/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class RepeatedUseECSignedPreKeyStore extends RepeatedUseSignedPreKeyStore<ECSignedPreKey> {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  public RepeatedUseECSignedPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    super(dynamoDbAsyncClient, tableName);

    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  @Override
  protected Map<String, AttributeValue> getItemFromPreKey(final UUID accountUuid, final long deviceId, final ECSignedPreKey signedPreKey) {

    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(accountUuid),
        KEY_DEVICE_ID, getSortKey(deviceId),
        ATTR_KEY_ID, AttributeValues.fromLong(signedPreKey.keyId()),
        ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(signedPreKey.serializedPublicKey()),
        ATTR_SIGNATURE, AttributeValues.fromByteArray(signedPreKey.signature()));
  }

  @Override
  protected ECSignedPreKey getPreKeyFromItem(final Map<String, AttributeValue> item) {
    try {
      return new ECSignedPreKey(
          Long.parseLong(item.get(ATTR_KEY_ID).n()),
          new ECPublicKey(item.get(ATTR_PUBLIC_KEY).b().asByteArray()),
          item.get(ATTR_SIGNATURE).b().asByteArray());
    } catch (final InvalidKeyException e) {
      // This should never happen since we're serializing keys directly from `ECPublicKey` instances on the way in
      throw new IllegalArgumentException(e);
    }
  }

  public CompletableFuture<Boolean> storeIfAbsent(final UUID identifier, final long deviceId, final ECSignedPreKey signedPreKey) {
    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(getItemFromPreKey(identifier, deviceId, signedPreKey))
            .conditionExpression("attribute_not_exists(#public_key)")
            .expressionAttributeNames(Map.of("#public_key", ATTR_PUBLIC_KEY))
            .build())
        .thenApply(ignored -> true)
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            return false;
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }
}
