/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public class KeysManager {

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final SingleUseECPreKeyStore ecPreKeys;
  private final SingleUseKEMPreKeyStore pqPreKeys;
  private final RepeatedUseECSignedPreKeyStore ecSignedPreKeys;
  private final RepeatedUseKEMSignedPreKeyStore pqLastResortKeys;

  public KeysManager(
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String ecTableName,
      final String pqTableName,
      final String ecSignedPreKeysTableName,
      final String pqLastResortTableName,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.ecPreKeys = new SingleUseECPreKeyStore(dynamoDbAsyncClient, ecTableName);
    this.pqPreKeys = new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, pqTableName);
    this.ecSignedPreKeys = new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient, ecSignedPreKeysTableName);
    this.pqLastResortKeys = new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient, pqLastResortTableName);
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public Optional<TransactWriteItem> buildWriteItemForEcSignedPreKey(final UUID identifier,
      final byte deviceId,
      final ECSignedPreKey ecSignedPreKey) {

    return dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().storeEcSignedPreKeys()
        ? Optional.of(ecSignedPreKeys.buildTransactWriteItem(identifier, deviceId, ecSignedPreKey))
        : Optional.empty();
  }

  public TransactWriteItem buildWriteItemForLastResortKey(final UUID identifier,
      final byte deviceId,
      final KEMSignedPreKey lastResortSignedPreKey) {

    return pqLastResortKeys.buildTransactWriteItem(identifier, deviceId, lastResortSignedPreKey);
  }

  public List<TransactWriteItem> buildWriteItemsForRepeatedUseKeys(final UUID accountIdentifier,
      final UUID phoneNumberIdentifier,
      final byte deviceId,
      final ECSignedPreKey aciSignedPreKey,
      final ECSignedPreKey pniSignedPreKey,
      final KEMSignedPreKey aciPqLastResortPreKey,
      final KEMSignedPreKey pniLastResortPreKey) {

    return List.of(ecSignedPreKeys.buildTransactWriteItem(accountIdentifier, deviceId, aciSignedPreKey),
        ecSignedPreKeys.buildTransactWriteItem(phoneNumberIdentifier, deviceId, pniSignedPreKey),
        pqLastResortKeys.buildTransactWriteItem(accountIdentifier, deviceId, aciPqLastResortPreKey),
        pqLastResortKeys.buildTransactWriteItem(phoneNumberIdentifier, deviceId, pniLastResortPreKey));
  }

  public CompletableFuture<Void> storeEcSignedPreKeys(final UUID identifier, final Map<Byte, ECSignedPreKey> keys) {
    if (dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().storeEcSignedPreKeys()) {
      return ecSignedPreKeys.store(identifier, keys);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public CompletableFuture<Boolean> storeEcSignedPreKeyIfAbsent(final UUID identifier, final byte deviceId,
          final ECSignedPreKey signedPreKey) {
    return ecSignedPreKeys.storeIfAbsent(identifier, deviceId, signedPreKey);
  }

  public CompletableFuture<Void> storePqLastResort(final UUID identifier, final Map<Byte, KEMSignedPreKey> keys) {
    return pqLastResortKeys.store(identifier, keys);
  }

  public CompletableFuture<Void> storeEcOneTimePreKeys(final UUID identifier, final byte deviceId,
          final List<ECPreKey> preKeys) {
    return ecPreKeys.store(identifier, deviceId, preKeys);
  }

  public CompletableFuture<Void> storeKemOneTimePreKeys(final UUID identifier, final byte deviceId,
          final List<KEMSignedPreKey> preKeys) {
    return pqPreKeys.store(identifier, deviceId, preKeys);
  }

  public CompletableFuture<Optional<ECPreKey>> takeEC(final UUID identifier, final byte deviceId) {
    return ecPreKeys.take(identifier, deviceId);
  }

  public CompletableFuture<Optional<KEMSignedPreKey>> takePQ(final UUID identifier, final byte deviceId) {
    return pqPreKeys.take(identifier, deviceId)
        .thenCompose(maybeSingleUsePreKey -> maybeSingleUsePreKey
            .map(singleUsePreKey -> CompletableFuture.completedFuture(maybeSingleUsePreKey))
            .orElseGet(() -> pqLastResortKeys.find(identifier, deviceId)));
  }

  @VisibleForTesting
  CompletableFuture<Optional<KEMSignedPreKey>> getLastResort(final UUID identifier, final byte deviceId) {
    return pqLastResortKeys.find(identifier, deviceId);
  }

  public CompletableFuture<Optional<ECSignedPreKey>> getEcSignedPreKey(final UUID identifier, final byte deviceId) {
    return ecSignedPreKeys.find(identifier, deviceId);
  }

  public CompletableFuture<List<Byte>> getPqEnabledDevices(final UUID identifier) {
    return pqLastResortKeys.getDeviceIdsWithKeys(identifier).collectList().toFuture();
  }

  public CompletableFuture<Integer> getEcCount(final UUID identifier, final byte deviceId) {
    return ecPreKeys.getCount(identifier, deviceId);
  }

  public CompletableFuture<Integer> getPqCount(final UUID identifier, final byte deviceId) {
    return pqPreKeys.getCount(identifier, deviceId);
  }

  public CompletableFuture<Void> delete(final UUID identifier) {
    return delete(identifier, false);
  }

  public CompletableFuture<Void> delete(final UUID identifier, final boolean excludePrimaryDevice) {
    return CompletableFuture.allOf(
        ecPreKeys.delete(identifier),
        pqPreKeys.delete(identifier),
        dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().deleteEcSignedPreKeys()
            ? ecSignedPreKeys.delete(identifier, excludePrimaryDevice)
            : CompletableFuture.completedFuture(null),
        pqLastResortKeys.delete(identifier, excludePrimaryDevice));
  }

  public CompletableFuture<Void> delete(final UUID accountUuid, final byte deviceId) {
    return CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid, deviceId),
            pqPreKeys.delete(accountUuid, deviceId),
            dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().deleteEcSignedPreKeys()
                ? ecSignedPreKeys.delete(accountUuid, deviceId)
                : CompletableFuture.completedFuture(null),
            pqLastResortKeys.delete(accountUuid, deviceId));
  }
}
