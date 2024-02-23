/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public class KeysManager {

  private final SingleUseECPreKeyStore ecPreKeys;
  private final SingleUseKEMPreKeyStore pqPreKeys;
  private final RepeatedUseECSignedPreKeyStore ecSignedPreKeys;
  private final RepeatedUseKEMSignedPreKeyStore pqLastResortKeys;

  public KeysManager(
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String ecTableName,
      final String pqTableName,
      final String ecSignedPreKeysTableName,
      final String pqLastResortTableName) {
    this.ecPreKeys = new SingleUseECPreKeyStore(dynamoDbAsyncClient, ecTableName);
    this.pqPreKeys = new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, pqTableName);
    this.ecSignedPreKeys = new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient, ecSignedPreKeysTableName);
    this.pqLastResortKeys = new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient, pqLastResortTableName);
  }

  public TransactWriteItem buildWriteItemForEcSignedPreKey(final UUID identifier,
      final byte deviceId,
      final ECSignedPreKey ecSignedPreKey) {

    return ecSignedPreKeys.buildTransactWriteItemForInsertion(identifier, deviceId, ecSignedPreKey);
  }

  public TransactWriteItem buildWriteItemForLastResortKey(final UUID identifier,
      final byte deviceId,
      final KEMSignedPreKey lastResortSignedPreKey) {

    return pqLastResortKeys.buildTransactWriteItemForInsertion(identifier, deviceId, lastResortSignedPreKey);
  }

  public List<TransactWriteItem> buildWriteItemsForNewDevice(final UUID accountIdentifier,
      final UUID phoneNumberIdentifier,
      final byte deviceId,
      final ECSignedPreKey aciSignedPreKey,
      final ECSignedPreKey pniSignedPreKey,
      final KEMSignedPreKey aciPqLastResortPreKey,
      final KEMSignedPreKey pniLastResortPreKey) {

    return List.of(
        ecSignedPreKeys.buildTransactWriteItemForInsertion(accountIdentifier, deviceId, aciSignedPreKey),
        ecSignedPreKeys.buildTransactWriteItemForInsertion(phoneNumberIdentifier, deviceId, pniSignedPreKey),
        pqLastResortKeys.buildTransactWriteItemForInsertion(accountIdentifier, deviceId, aciPqLastResortPreKey),
        pqLastResortKeys.buildTransactWriteItemForInsertion(phoneNumberIdentifier, deviceId, pniLastResortPreKey)
    );
  }

  public List<TransactWriteItem> buildWriteItemsForRemovedDevice(final UUID accountIdentifier,
      final UUID phoneNumberIdentifier,
      final byte deviceId) {

    return List.of(
        ecSignedPreKeys.buildTransactWriteItemForDeletion(accountIdentifier, deviceId),
        ecSignedPreKeys.buildTransactWriteItemForDeletion(phoneNumberIdentifier, deviceId),
        pqLastResortKeys.buildTransactWriteItemForDeletion(accountIdentifier, deviceId),
        pqLastResortKeys.buildTransactWriteItemForDeletion(phoneNumberIdentifier, deviceId)
    );
  }

  public CompletableFuture<Void> storeEcSignedPreKeys(final UUID identifier, final byte deviceId, final ECSignedPreKey ecSignedPreKey) {
    return ecSignedPreKeys.store(identifier, deviceId, ecSignedPreKey);
  }

  public CompletableFuture<Void> storePqLastResort(final UUID identifier, final byte deviceId, final KEMSignedPreKey lastResortKey) {
    return pqLastResortKeys.store(identifier, deviceId, lastResortKey);
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

  public CompletableFuture<Optional<KEMSignedPreKey>> getLastResort(final UUID identifier, final byte deviceId) {
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

  public CompletableFuture<Void> deleteSingleUsePreKeys(final UUID identifier) {
    return CompletableFuture.allOf(
        ecPreKeys.delete(identifier),
        pqPreKeys.delete(identifier)
    );
  }

  public CompletableFuture<Void> deleteSingleUsePreKeys(final UUID accountUuid, final byte deviceId) {
    return CompletableFuture.allOf(
            ecPreKeys.delete(accountUuid, deviceId),
            pqPreKeys.delete(accountUuid, deviceId)
    );
  }
}
