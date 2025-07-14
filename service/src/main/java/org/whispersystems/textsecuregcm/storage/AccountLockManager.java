package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class AccountLockManager {

  private final AmazonDynamoDBLockClient lockClient;

  static final String KEY_ACCOUNT_PNI = "P";

  public AccountLockManager(final DynamoDbClient lockDynamoDb, final String lockTableName) {
    this(new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(lockDynamoDb, lockTableName)
            .withPartitionKeyName(KEY_ACCOUNT_PNI)
            .withLeaseDuration(15L)
            .withHeartbeatPeriod(2L)
            .withTimeUnit(TimeUnit.SECONDS)
            .withCreateHeartbeatBackgroundThread(true)
            .build()));
  }

  @VisibleForTesting
  AccountLockManager(final AmazonDynamoDBLockClient lockClient) {
    this.lockClient = lockClient;
  }

  /**
   * Acquires a distributed, pessimistic lock for the accounts identified by the given phone number identifiers. By
   * design, the accounts need not actually exist in order to acquire a lock; this allows lock acquisition for
   * operations that span account lifecycle changes (like deleting an account or changing a phone number). The given
   * task runs once locks for all given identifiers have been acquired, and the locks are released as soon as the task
   * completes by any means.
   *
   * @param phoneNumberIdentifiers  the phone number identifiers for which to acquire a distributed, pessimistic lock
   * @param task                    the task to execute once locks have been acquired
   * @param lockAcquisitionExecutor the executor on which to run blocking lock acquire/release tasks. this executor
   *                                should not use virtual threads.
   *
   * @return the value returned by the given {@code task}
   *
   * @throws Exception if an exception is thrown by the given {@code task}
   */
  public <V> V withLock(final Set<UUID> phoneNumberIdentifiers,
      final Callable<V> task,
      final Executor lockAcquisitionExecutor) throws Exception {

    if (phoneNumberIdentifiers.isEmpty()) {
      throw new IllegalArgumentException("List of PNIs to lock must not be empty");
    }

    final List<LockItem> lockItems = new ArrayList<>(phoneNumberIdentifiers.size());

    try {
      // Offload the acquire/release tasks to the dedicated lock acquisition executor. The lock client performs blocking
      // operations while holding locks which forces thread pinning when this method runs on a virtual thread.
      // https://github.com/awslabs/amazon-dynamodb-lock-client/issues/97
      CompletableFuture.runAsync(() -> {
        for (final UUID pni : phoneNumberIdentifiers) {
          try {
            lockItems.add(lockClient.acquireLock(AcquireLockOptions.builder(pni.toString())
                .withAcquireReleasedLocksConsistently(true)
                .build()));
          } catch (final InterruptedException e) {
            throw new CompletionException(e);
          }
        }
      }, lockAcquisitionExecutor).join();

      return task.call();
    } finally {
      CompletableFuture.runAsync(() -> {
        for (final LockItem lockItem : lockItems) {
          lockClient.releaseLock(ReleaseLockOptions.builder(lockItem)
              .withBestEffort(true)
              .build());
        }
      }, lockAcquisitionExecutor).join();
    }
  }

  /**
   * Acquires a distributed, pessimistic lock for the accounts identified by the given phone number identifiers. By
   * design, the accounts need not actually exist in order to acquire a lock; this allows lock acquisition for
   * operations that span account lifecycle changes (like deleting an account or changing a phone number). The given
   * task runs once locks for all given identifiers have been acquired, and the locks are released as soon as the task
   * completes by any means.
   *
   * @param phoneNumberIdentifiers the phone number identifiers for which to acquire a distributed, pessimistic lock
   * @param taskSupplier           a supplier for the task to execute once locks have been acquired
   * @param executor               the executor on which to acquire and release locks
   * @return a future that completes normally when the given task has executed successfully and all locks have been
   * released; the returned future may fail with an {@link InterruptedException} if interrupted while acquiring a lock
   */
  public <T> CompletableFuture<T> withLockAsync(final Set<UUID> phoneNumberIdentifiers,
      final Supplier<CompletableFuture<T>> taskSupplier, final Executor executor) {

    if (phoneNumberIdentifiers.isEmpty()) {
      throw new IllegalArgumentException("List of PNIs to lock must not be empty");
    }

    final List<LockItem> lockItems = new ArrayList<>(phoneNumberIdentifiers.size());

    return CompletableFuture.runAsync(() -> {
          for (final UUID pni : phoneNumberIdentifiers) {
            try {
              lockItems.add(lockClient.acquireLock(AcquireLockOptions.builder(pni.toString())
                  .withAcquireReleasedLocksConsistently(true)
                  .build()));
            } catch (final InterruptedException e) {
              throw new CompletionException(e);
            }
          }
        }, executor)
        .thenCompose(ignored -> taskSupplier.get())
        .whenCompleteAsync((ignored, throwable) -> lockItems.forEach(lockItem -> lockClient.releaseLock(ReleaseLockOptions.builder(lockItem)
            .withBestEffort(true)
            .build())), executor);
  }
}
