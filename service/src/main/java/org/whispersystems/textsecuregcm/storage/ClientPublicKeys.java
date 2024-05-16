package org.whispersystems.textsecuregcm.storage;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

/**
 * Stores clients' public keys for transport-level authentication/encryption in a DynamoDB table.
 */
public class ClientPublicKeys {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  static final String KEY_ACCOUNT_UUID = "U";
  static final String KEY_DEVICE_ID = "D";
  static final String ATTR_PUBLIC_KEY = "K";

  private static final Logger log = LoggerFactory.getLogger(ClientPublicKeys.class);

  public ClientPublicKeys(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  /**
   * Stores the given public key for the given account/device, overwriting any previously-stored public key. This method
   * is intended for use for adding public keys to existing accounts/devices as a migration step. Callers should use
   * {@link #buildTransactWriteItemForInsertion(UUID, byte, ECPublicKey)} instead when creating new accounts/devices.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   * @param publicKey the public key to store for the target account/device

   * @return a future that completes when the given key has been stored
   */
  CompletableFuture<Void> setPublicKey(final UUID accountIdentifier, final byte deviceId, final ECPublicKey publicKey) {
    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, getPartitionKey(accountIdentifier),
                KEY_DEVICE_ID, getSortKey(deviceId),
                ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(publicKey.serialize())))
            .build())
        .thenRun(Util.NOOP);
  }

  /**
   * Builds a {@link TransactWriteItem} that will store a public key for the given account/device. Intended for use when
   * adding devices to accounts or creating new accounts.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   * @param publicKey the public key to store for the target account/device
   *
   * @return a {@code TransactWriteItem} that will store the given public key for the given account/device
   */
  TransactWriteItem buildTransactWriteItemForInsertion(final UUID accountIdentifier,
      final byte deviceId,
      final ECPublicKey publicKey) {

    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, getPartitionKey(accountIdentifier),
                KEY_DEVICE_ID, getSortKey(deviceId),
                ATTR_PUBLIC_KEY, AttributeValues.fromByteArray(publicKey.serialize())))
            .build())
        .build();
  }

  /**
   * Builds a {@link TransactWriteItem} that will remove the public key for the given account/device. Intended for
   * use when removing devices from accounts or deleting/re-creating accounts.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   *
   * @return a {@code TransactWriteItem} that will remove the public key for the given account/device
   */
  TransactWriteItem buildTransactWriteItemForDeletion(final UUID accountIdentifier, final byte deviceId) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(tableName)
            .key(getPrimaryKey(accountIdentifier, deviceId))
            .build())
        .build();
  }

  /**
   * Finds the public key for the given account/device.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   *
   * @return a future that yields the Ed25519 public key for the given account/device, or empty if no public key was
   * found
   */
  CompletableFuture<Optional<ECPublicKey>> findPublicKey(final UUID accountIdentifier, final byte deviceId) {
    return dynamoDbAsyncClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .consistentRead(true)
        .key(getPrimaryKey(accountIdentifier, deviceId))
        .build())
        .thenApply(response -> Optional.of(response.item())
            .filter(item -> response.hasItem())
            .map(item -> {
              try {
                return new ECPublicKey(item.get(ATTR_PUBLIC_KEY).b().asByteArray());
              } catch (final InvalidKeyException e) {
                log.warn("Invalid public key for {}:{}", accountIdentifier, deviceId, e);
                return null;
              }
            }));
  }

  private static Map<String, AttributeValue> getPrimaryKey(final UUID identifier, final byte deviceId) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID, getSortKey(deviceId));
  }

  private static AttributeValue getPartitionKey(final UUID accountUuid) {
    return AttributeValues.fromUUID(accountUuid);
  }

  private static AttributeValue getSortKey(final byte deviceId) {
    return AttributeValues.fromInt(deviceId);
  }
}
