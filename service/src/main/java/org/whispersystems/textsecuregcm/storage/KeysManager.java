/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.util.Futures;
import org.whispersystems.textsecuregcm.util.Optionals;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import javax.annotation.Nullable;

public class KeysManager {
  // KeysController for backwards compatibility
  private static final String GET_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "getKeys");

  private final SingleUseECPreKeyStore ecPreKeys;
  private final SingleUseKEMPreKeyStore pqPreKeys;
  private final PagedSingleUseKEMPreKeyStore pagedPqPreKeys;
  private final RepeatedUseECSignedPreKeyStore ecSignedPreKeys;
  private final RepeatedUseKEMSignedPreKeyStore pqLastResortKeys;

  private static final String  TAKE_PQ_NAME = MetricsUtil.name(KeysManager.class, "takePq");

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

  public CompletableFuture<Void> storeEcSignedPreKeys(final UUID identifier, final byte deviceId,
      final ECSignedPreKey ecSignedPreKey) {
    return ecSignedPreKeys.store(identifier, deviceId, ecSignedPreKey);
  }

  public CompletableFuture<Void> storePqLastResort(final UUID identifier, final byte deviceId,
      final KEMSignedPreKey lastResortKey) {
    return pqLastResortKeys.store(identifier, deviceId, lastResortKey);
  }

  public CompletableFuture<Void> storeEcOneTimePreKeys(final UUID identifier, final byte deviceId,
      final List<ECPreKey> preKeys) {
    return ecPreKeys.store(identifier, deviceId, preKeys);
  }

  public CompletableFuture<Void> storeKemOneTimePreKeys(final UUID identifier, final byte deviceId,
      final List<KEMSignedPreKey> preKeys) {
    // Unconditionally delete keys in old format keystore, then write to the pagedPqPreKeys store
    return pqPreKeys.delete(identifier, deviceId)
        .thenCompose(_ -> pagedPqPreKeys.store(identifier, deviceId, preKeys));

  }

  @VisibleForTesting
  CompletableFuture<Optional<ECPreKey>> takeEC(final UUID identifier, final byte deviceId) {
    return ecPreKeys.take(identifier, deviceId);
  }

  @VisibleForTesting
  CompletableFuture<Optional<KEMSignedPreKey>> takePQ(final UUID identifier, final byte deviceId) {
    return tagTakePQ(pagedPqPreKeys.take(identifier, deviceId), PQSource.PAGE)
        .thenCompose(maybeSingleUsePreKey -> maybeSingleUsePreKey
              .map(ignored -> CompletableFuture.completedFuture(maybeSingleUsePreKey))
              .orElseGet(() -> tagTakePQ(pqPreKeys.take(identifier, deviceId), PQSource.ROW)))
        .thenCompose(maybeSingleUsePreKey -> maybeSingleUsePreKey
            .map(singleUsePreKey -> CompletableFuture.completedFuture(maybeSingleUsePreKey))
            .orElseGet(() -> tagTakePQ(pqLastResortKeys.find(identifier, deviceId), PQSource.LAST_RESORT)));
  }

  private enum PQSource {
    PAGE,
    ROW,
    LAST_RESORT
  }
  private CompletableFuture<Optional<KEMSignedPreKey>> tagTakePQ(CompletableFuture<Optional<KEMSignedPreKey>> prekey, final PQSource source) {
    return prekey.thenApply(maybeSingleUsePreKey -> {
      final Optional<String> maybeSourceTag = maybeSingleUsePreKey
          // If we found a PK, use this source tag
          .map(ignore -> source.name())
          // If we didn't and this is our last resort, we didn't find a PK
          .or(() -> source == PQSource.LAST_RESORT ? Optional.of("absent") : Optional.empty());
      maybeSourceTag.ifPresent(sourceTag -> {
        Metrics.counter(TAKE_PQ_NAME, "source", sourceTag).increment();
      });
      return maybeSingleUsePreKey;
    });
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
    // We only return the paged prekey count to encourage clients to upload more prekeys if they only have prekeys
    // stored in the previous key store format
    return pagedPqPreKeys.getCount(identifier, deviceId);
  }

  public CompletableFuture<Void> deleteSingleUsePreKeys(final UUID identifier) {
    return CompletableFuture.allOf(
        ecPreKeys.delete(identifier),
        pqPreKeys.delete(identifier),
        pagedPqPreKeys.delete(identifier)
    );
  }

  public CompletableFuture<Void> deleteSingleUsePreKeys(final UUID accountUuid, final byte deviceId) {
    return CompletableFuture.allOf(
        ecPreKeys.delete(accountUuid, deviceId),
        pqPreKeys.delete(accountUuid, deviceId),
        pagedPqPreKeys.delete(accountUuid, deviceId)
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

  public record DevicePreKeys(
      ECSignedPreKey ecSignedPreKey,
      Optional<ECPreKey> ecPreKey,
      KEMSignedPreKey kemSignedPreKey) {}

  public CompletableFuture<Optional<DevicePreKeys>> takeDevicePreKeys(
      final byte deviceId,
      final ServiceIdentifier serviceIdentifier,
      final @Nullable String userAgent) {
    final UUID uuid = serviceIdentifier.uuid();
    return Futures.zipWith(
            this.takeEC(uuid, deviceId),
            this.getEcSignedPreKey(uuid, deviceId),
            this.takePQ(uuid, deviceId),
            (maybeUnsignedEcPreKey, maybeSignedEcPreKey, maybePqPreKey) -> {

              Metrics.counter(GET_KEYS_COUNTER_NAME, Tags.of(
                      UserAgentTagUtil.getPlatformTag(userAgent),
                      Tag.of("identityType", serviceIdentifier.identityType().name()),
                      Tag.of("oneTimeEcKeyAvailable", String.valueOf(maybeUnsignedEcPreKey.isPresent())),
                      Tag.of("signedEcKeyAvailable", String.valueOf(maybeSignedEcPreKey.isPresent())),
                      Tag.of("pqKeyAvailable", String.valueOf(maybePqPreKey.isPresent()))))
                  .increment();

              // The pq prekey and signed EC prekey should never be null for an existing account. This should only happen
              // if the account or device has been removed and the read was split, so we can return empty in those cases.
              return Optionals.zipWith(maybeSignedEcPreKey, maybePqPreKey, (signedEcPreKey, pqPreKey) ->
                  new DevicePreKeys(signedEcPreKey, maybeUnsignedEcPreKey, pqPreKey));
            })
        .toCompletableFuture();
  }
}
