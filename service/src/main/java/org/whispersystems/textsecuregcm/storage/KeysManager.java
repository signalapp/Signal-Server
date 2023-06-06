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
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class KeysManager {

  private final SingleUseECPreKeyStore ecPreKeys;
  private final SingleUseKEMPreKeyStore pqPreKeys;
  private final RepeatedUseSignedPreKeyStore pqLastResortKeys;

  public KeysManager(
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String ecTableName,
      final String pqTableName,
      final String pqLastResortTableName) {
    this.ecPreKeys = new SingleUseECPreKeyStore(dynamoDbAsyncClient, ecTableName);
    this.pqPreKeys = new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, pqTableName);
    this.pqLastResortKeys = new RepeatedUseSignedPreKeyStore(dynamoDbAsyncClient, pqLastResortTableName);
  }

  public void store(final UUID identifier, final long deviceId, final List<PreKey> keys) {
    store(identifier, deviceId, keys, null, null);
  }

  public void store(
      final UUID identifier, final long deviceId,
      @Nullable final List<PreKey> ecKeys,
      @Nullable final List<SignedPreKey> pqKeys,
      @Nullable final SignedPreKey pqLastResortKey) {

    final List<CompletableFuture<Void>> storeFutures = new ArrayList<>();

    if (ecKeys != null && !ecKeys.isEmpty()) {
      storeFutures.add(ecPreKeys.store(identifier, deviceId, ecKeys));
    }

    if (pqKeys != null && !pqKeys.isEmpty()) {
      storeFutures.add(pqPreKeys.store(identifier, deviceId, pqKeys));
    }

    if (pqLastResortKey != null) {
      storeFutures.add(pqLastResortKeys.store(identifier, deviceId, pqLastResortKey));
    }

    CompletableFuture.allOf(storeFutures.toArray(new CompletableFuture[0])).join();
  }

  public void storePqLastResort(final UUID identifier, final Map<Long, SignedPreKey> keys) {
    pqLastResortKeys.store(identifier, keys).join();
  }

  public Optional<PreKey> takeEC(final UUID identifier, final long deviceId) {
    return ecPreKeys.take(identifier, deviceId).join();
  }

  public Optional<SignedPreKey> takePQ(final UUID identifier, final long deviceId) {
    return pqPreKeys.take(identifier, deviceId)
        .thenCompose(maybeSingleUsePreKey -> maybeSingleUsePreKey
            .map(singleUsePreKey -> CompletableFuture.completedFuture(maybeSingleUsePreKey))
            .orElseGet(() -> pqLastResortKeys.find(identifier, deviceId))).join();
  }

  @VisibleForTesting
  Optional<PreKey> getLastResort(final UUID identifier, final long deviceId) {
    return pqLastResortKeys.find(identifier, deviceId).join()
        .map(signedPreKey -> signedPreKey);
  }

  public List<Long> getPqEnabledDevices(final UUID identifier) {
    return pqLastResortKeys.getDeviceIdsWithKeys(identifier).collectList().block();
  }

  public int getEcCount(final UUID identifier, final long deviceId) {
    return ecPreKeys.getCount(identifier, deviceId).join();
  }

  public int getPqCount(final UUID identifier, final long deviceId) {
    return pqPreKeys.getCount(identifier, deviceId).join();
  }
  
  public void delete(final UUID accountUuid) {
    CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid),
            pqPreKeys.delete(accountUuid),
            pqLastResortKeys.delete(accountUuid))
        .join();
  }

  public void delete(final UUID accountUuid, final long deviceId) {
    CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid, deviceId),
            pqPreKeys.delete(accountUuid, deviceId),
            pqLastResortKeys.delete(accountUuid, deviceId))
        .join();
  }
}
