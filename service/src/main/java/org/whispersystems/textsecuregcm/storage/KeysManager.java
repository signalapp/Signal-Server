/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public class KeysManager {

  private final SingleUseECPreKeyStore ecPreKeys;
  private final SingleUseKEMPreKeyStore pqPreKeys;
  private final PagedSingleUseKEMPreKeyStore pagedPqPreKeys;
  private final RepeatedUseECSignedPreKeyStore ecSignedPreKeys;
  private final RepeatedUseKEMSignedPreKeyStore pqLastResortKeys;

  public KeysManager(
      final SingleUseECPreKeyStore ecPreKeys,
      final SingleUseKEMPreKeyStore pqPreKeys,
      final PagedSingleUseKEMPreKeyStore pagedPqPreKeys,
      final RepeatedUseECSignedPreKeyStore ecSignedPreKeys,
      final RepeatedUseKEMSignedPreKeyStore pqLastResortKeys) {
    this.ecPreKeys = ecPreKeys;
    this.pqPreKeys = pqPreKeys;
    this.pagedPqPreKeys = pagedPqPreKeys;
    this.ecSignedPreKeys = ecSignedPreKeys;
    this.pqLastResortKeys = pqLastResortKeys;
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

  /**
   * List all the current remotely stored prekey pages across all devices. Pages that are no longer in use can be
   * removed with {@link #pruneDeadPage}
   *
   * @param lookupConcurrency the number of concurrent lookup operations to perform when populating list results
   * @return All stored prekey pages
   */
  public Flux<DeviceKEMPreKeyPages> listStoredKEMPreKeyPages(int lookupConcurrency) {
    return pagedPqPreKeys.listStoredPages(lookupConcurrency);
  }

  /**
   * Remove a prekey page that is no longer in use. A page should only be removed if it is not the active page and
   * it has no chance of being updated to be.
   *
   * @param identifier The owner of the dead page
   * @param deviceId The device of the dead page
   * @param pageId The dead page to remove from storage
   * @return A future that completes when the page has been removed
   */
  public CompletableFuture<Void> pruneDeadPage(final UUID identifier, final byte deviceId, final UUID pageId) {
    return pagedPqPreKeys.deleteBundleFromS3(identifier, deviceId, pageId);
  }
}
