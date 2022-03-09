/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Pair;

public class DeletedAccountsManager {

  private final DeletedAccounts deletedAccounts;

  private final AmazonDynamoDBLockClient lockClient;

  private static final Logger log = LoggerFactory.getLogger(DeletedAccountsManager.class);

  @FunctionalInterface
  public interface DeletedAccountReconciliationConsumer {

    /**
     * Reconcile a list of deleted account records.
     *
     * @param deletedAccounts the account records to reconcile
     * @return a list of account records that were successfully reconciled; accounts that were not successfully
     * reconciled may be retried later
     * @throws ChunkProcessingFailedException in the event of an error while processing the batch of account records
     */
    Collection<String> reconcile(List<Pair<UUID, String>> deletedAccounts) throws ChunkProcessingFailedException;
  }

  public DeletedAccountsManager(final DeletedAccounts deletedAccounts, final AmazonDynamoDB lockDynamoDb, final String lockTableName) {
    this.deletedAccounts = deletedAccounts;

    lockClient = new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(lockDynamoDb, lockTableName)
            .withPartitionKeyName(DeletedAccounts.KEY_ACCOUNT_E164)
            .withLeaseDuration(15L)
            .withHeartbeatPeriod(2L)
            .withTimeUnit(TimeUnit.SECONDS)
            .withCreateHeartbeatBackgroundThread(true)
            .build());
  }

  /**
   * Acquires a pessimistic lock for the given phone number and performs the given action, passing the UUID of the
   * recently-deleted account (if any) that previously held the given number.
   *
   * @param e164 the phone number to lock and with which to perform an action
   * @param consumer the action to take; accepts the UUID of the account that previously held the given e164, if any,
   * as an argument
   *
   * @throws InterruptedException if interrupted while waiting to acquire a lock on the given phone number
   */
  public void lockAndTake(final String e164, final Consumer<Optional<UUID>> consumer) throws InterruptedException {
    withLock(List.of(e164), acis -> {
      try {
        consumer.accept(acis.get(0));
        deletedAccounts.remove(e164);
      } catch (final Exception e) {
        log.warn("Consumer threw an exception while holding lock on a deleted account record", e);
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Acquires a pessimistic lock for the given phone number and performs an action that deletes an account, returning
   * the UUID of the deleted account. The UUID of the deleted account will be stored in the list of recently-deleted
   * e164-to-UUID mappings.
   *
   * @param e164 the phone number to lock and with which to perform an action
   * @param supplier the deletion action to take on the account associated with the given number; must return the UUID
   * of the deleted account
   *
   * @throws InterruptedException if interrupted while waiting to acquire a lock on the given phone number
   */
  public void lockAndPut(final String e164, final Supplier<UUID> supplier) throws InterruptedException {
    withLock(List.of(e164), ignored -> {
      try {
        deletedAccounts.put(supplier.get(), e164, true);
      } catch (final Exception e) {
        log.warn("Supplier threw an exception while holding lock on a deleted account record", e);
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Acquires a pessimistic lock for the given phone numbers and performs an action that may or may not delete an
   * account associated with the target number. The UUID of the deleted account (if any) will be stored in the list of
   * recently-deleted e164-to-UUID mappings.
   *
   * @param original the phone number of an existing account to lock and with which to perform an action
   * @param target   the phone number of an account that may or may not exist with which to perform an action
   * @param function the action to take on the given phone numbers and ACIs, if known; the action may delete the account
   *                 identified by the target number, in which case it must return the ACI of that account
   * @throws InterruptedException if interrupted while waiting to acquire a lock on the given phone numbers
   */
  public void lockAndPut(final String original, final String target,
      final BiFunction<Optional<UUID>, Optional<UUID>, Optional<UUID>> function)
      throws InterruptedException {

    withLock(List.of(original, target), acis -> {
      try {
        function.apply(acis.get(0), acis.get(1)).ifPresent(aci -> deletedAccounts.put(aci, original, true));
      } catch (final Exception e) {
        log.warn("Supplier threw an exception while holding lock on a deleted account record", e);
        throw new RuntimeException(e);
      }
    });
  }

  private void withLock(final List<String> e164s, final Consumer<List<Optional<UUID>>> task)
      throws InterruptedException {
    final List<LockItem> lockItems = new ArrayList<>(e164s.size());

    try {
      final List<Optional<UUID>> previouslyDeletedUuids = new ArrayList<>(e164s.size());
      for (final String e164 : e164s) {
        lockItems.add(lockClient.acquireLock(AcquireLockOptions.builder(e164)
            .withAcquireReleasedLocksConsistently(true)
            .build()));
        previouslyDeletedUuids.add(deletedAccounts.findUuid(e164));
      }

      task.accept(previouslyDeletedUuids);
    } finally {
      for (final LockItem lockItem : lockItems) {
        lockClient.releaseLock(ReleaseLockOptions.builder(lockItem)
            .withBestEffort(true)
            .build());
      }
    }
  }

  public void lockAndReconcileAccounts(final int max, final DeletedAccountReconciliationConsumer consumer) throws ChunkProcessingFailedException {
    final List<LockItem> lockItems = new ArrayList<>();
    final List<Pair<UUID, String>> reconciliationCandidates = deletedAccounts.listAccountsToReconcile(max).stream()
        .filter(pair -> {
          boolean lockAcquired = false;

          try {
            lockItems.add(lockClient.acquireLock(AcquireLockOptions.builder(pair.second())
                .withAcquireReleasedLocksConsistently(true)
                .withShouldSkipBlockingWait(true)
                .build()));

            lockAcquired = true;
          } catch (final InterruptedException e) {
            log.warn("Interrupted while acquiring lock for reconciliation", e);
          } catch (final LockCurrentlyUnavailableException ignored) {
          }

          return lockAcquired;
        }).toList();

    assert lockItems.size() == reconciliationCandidates.size();

    // A deleted account's status may have changed in the time between getting a list of candidates and acquiring a lock
    // on the candidate records. Now that we hold the lock, check which of the candidates still need to be reconciled.
    final Set<String> numbersNeedingReconciliationAfterLock =
        deletedAccounts.getAccountsNeedingReconciliation(reconciliationCandidates.stream()
            .map(Pair::second)
            .collect(Collectors.toList()));

    final List<Pair<UUID, String>> accountsToReconcile = reconciliationCandidates.stream()
        .filter(candidate -> numbersNeedingReconciliationAfterLock.contains(candidate.second()))
        .collect(Collectors.toList());

    try {
      deletedAccounts.markReconciled(consumer.reconcile(accountsToReconcile));
    } finally {
      lockItems.forEach(
          lockItem -> lockClient.releaseLock(ReleaseLockOptions.builder(lockItem).withBestEffort(true).build()));
    }
  }

  public Optional<UUID> findDeletedAccountAci(final String e164) {
    return deletedAccounts.findUuid(e164);
  }

  public Optional<String> findDeletedAccountE164(final UUID uuid) {
    return deletedAccounts.findE164(uuid);
  }
}
