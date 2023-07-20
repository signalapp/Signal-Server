/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

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

  public CompletableFuture<Void> store(final UUID identifier, final long deviceId, final List<ECPreKey> keys) {
    return store(identifier, deviceId, keys, null, null, null);
  }

  public CompletableFuture<Void> store(
      final UUID identifier, final long deviceId,
      @Nullable final List<ECPreKey> ecKeys,
      @Nullable final List<KEMSignedPreKey> pqKeys,
      @Nullable final ECSignedPreKey ecSignedPreKey,
      @Nullable final KEMSignedPreKey pqLastResortKey) {

    final List<CompletableFuture<Void>> storeFutures = new ArrayList<>();

    if (ecKeys != null && !ecKeys.isEmpty()) {
      storeFutures.add(ecPreKeys.store(identifier, deviceId, ecKeys));
    }

    if (pqKeys != null && !pqKeys.isEmpty()) {
      storeFutures.add(pqPreKeys.store(identifier, deviceId, pqKeys));
    }

    if (ecSignedPreKey != null && dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().storeEcSignedPreKeys()) {
      storeFutures.add(ecSignedPreKeys.store(identifier, deviceId, ecSignedPreKey));
    }

    if (pqLastResortKey != null) {
      storeFutures.add(pqLastResortKeys.store(identifier, deviceId, pqLastResortKey));
    }

    return CompletableFuture.allOf(storeFutures.toArray(new CompletableFuture[0]));
  }

  public CompletableFuture<Void> storeEcSignedPreKeys(final UUID identifier, final Map<Long, ECSignedPreKey> keys) {
    if (dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().storeEcSignedPreKeys()) {
      return ecSignedPreKeys.store(identifier, keys);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public CompletableFuture<Boolean> storeEcSignedPreKeyIfAbsent(final UUID identifier, final long deviceId, final ECSignedPreKey signedPreKey) {
    return ecSignedPreKeys.storeIfAbsent(identifier, deviceId, signedPreKey);
  }

  public CompletableFuture<Void> storePqLastResort(final UUID identifier, final Map<Long, KEMSignedPreKey> keys) {
    return pqLastResortKeys.store(identifier, keys);
  }

  public CompletableFuture<Void> storeEcOneTimePreKeys(final UUID identifier, final long deviceId, final List<ECPreKey> preKeys) {
    return ecPreKeys.store(identifier, deviceId, preKeys);
  }

  public CompletableFuture<Void> storeKemOneTimePreKeys(final UUID identifier, final long deviceId, final List<KEMSignedPreKey> preKeys) {
    return pqPreKeys.store(identifier, deviceId, preKeys);
  }

  public CompletableFuture<Optional<ECPreKey>> takeEC(final UUID identifier, final long deviceId) {
    return ecPreKeys.take(identifier, deviceId);
  }

  public CompletableFuture<Optional<KEMSignedPreKey>> takePQ(final UUID identifier, final long deviceId) {
    return pqPreKeys.take(identifier, deviceId)
        .thenCompose(maybeSingleUsePreKey -> maybeSingleUsePreKey
            .map(singleUsePreKey -> CompletableFuture.completedFuture(maybeSingleUsePreKey))
            .orElseGet(() -> pqLastResortKeys.find(identifier, deviceId)));
  }

  @VisibleForTesting
  CompletableFuture<Optional<KEMSignedPreKey>> getLastResort(final UUID identifier, final long deviceId) {
    return pqLastResortKeys.find(identifier, deviceId);
  }

  public CompletableFuture<Optional<ECSignedPreKey>> getEcSignedPreKey(final UUID identifier, final long deviceId) {
    return ecSignedPreKeys.find(identifier, deviceId);
  }

  public CompletableFuture<List<Long>> getPqEnabledDevices(final UUID identifier) {
    return pqLastResortKeys.getDeviceIdsWithKeys(identifier).collectList().toFuture();
  }

  public CompletableFuture<Integer> getEcCount(final UUID identifier, final long deviceId) {
    return ecPreKeys.getCount(identifier, deviceId);
  }

  public CompletableFuture<Integer> getPqCount(final UUID identifier, final long deviceId) {
    return pqPreKeys.getCount(identifier, deviceId);
  }
  
  public CompletableFuture<Void> delete(final UUID accountUuid) {
    return CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid),
            pqPreKeys.delete(accountUuid),
            dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().deleteEcSignedPreKeys()
                ? ecSignedPreKeys.delete(accountUuid)
                : CompletableFuture.completedFuture(null),
            pqLastResortKeys.delete(accountUuid));
  }

  public CompletableFuture<Void> delete(final UUID accountUuid, final long deviceId) {
    return CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid, deviceId),
            pqPreKeys.delete(accountUuid, deviceId),
            dynamicConfigurationManager.getConfiguration().getEcPreKeyMigrationConfiguration().deleteEcSignedPreKeys()
                ? ecSignedPreKeys.delete(accountUuid, deviceId)
                : CompletableFuture.completedFuture(null),
            pqLastResortKeys.delete(accountUuid, deviceId));
  }
}
