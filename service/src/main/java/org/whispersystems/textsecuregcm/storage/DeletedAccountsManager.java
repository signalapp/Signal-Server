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

  public void lockAndTake(final String e164, final Consumer<Optional<UUID>> consumer) throws InterruptedException {
    withLock(e164, () -> {
      try {
        consumer.accept(deletedAccounts.findUuid(e164));
        deletedAccounts.remove(e164);
      } catch (final Exception e) {
        log.warn("Consumer threw an exception while holding lock on a deleted account record", e);
      }
    });
  }

  public void lockAndPut(final String e164, final Supplier<UUID> supplier) throws InterruptedException {
    withLock(e164, () -> {
      try {
        deletedAccounts.put(supplier.get(), e164, true);
      } catch (final Exception e) {
        log.warn("Supplier threw an exception while holding lock on a deleted account record", e);
      }
    });
  }

  private void withLock(final String e164, final Runnable task) throws InterruptedException {
    final LockItem lockItem = lockClient.acquireLock(AcquireLockOptions.builder(e164)
        .withAcquireReleasedLocksConsistently(true)
        .build());

    try {
      task.run();
    } finally {
      lockClient.releaseLock(ReleaseLockOptions.builder(lockItem)
          .withBestEffort(true)
          .build());
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
        })
        .collect(Collectors.toList());

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
      lockItems.forEach(lockItem -> lockClient.releaseLock(ReleaseLockOptions.builder(lockItem).withBestEffort(true).build()));
    }
  }
}
