package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class AccountLockManager {

  private final AmazonDynamoDBLockClient lockClient;

  static final String KEY_ACCOUNT_E164 = "P";

  public AccountLockManager(final DynamoDbClient lockDynamoDb, final String lockTableName) {
    this(new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(lockDynamoDb, lockTableName)
            .withPartitionKeyName(KEY_ACCOUNT_E164)
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
   * Acquires a distributed, pessimistic lock for the accounts identified by the given phone numbers. By design, the
   * accounts need not actually exist in order to acquire a lock; this allows lock acquisition for operations that span
   * account lifecycle changes (like deleting an account or changing a phone number). The given task runs once locks for
   * all given phone numbers have been acquired, and the locks are released as soon as the task completes by any means.
   *
   * @param e164s the phone numbers for which to acquire a distributed, pessimistic lock
   * @param task the task to execute once locks have been acquired
   *
   * @throws InterruptedException if interrupted while acquiring a lock
   */
  public void withLock(final List<String> e164s, final Runnable task) throws InterruptedException {
    if (e164s.isEmpty()) {
      throw new IllegalArgumentException("List of e164s to lock must not be empty");
    }

    final List<LockItem> lockItems = new ArrayList<>(e164s.size());

    try {
      for (final String e164 : e164s) {
        lockItems.add(lockClient.acquireLock(AcquireLockOptions.builder(e164)
            .withAcquireReleasedLocksConsistently(true)
            .build()));
      }

      task.run();
    } finally {
      lockItems.forEach(lockItem -> lockClient.releaseLock(ReleaseLockOptions.builder(lockItem)
          .withBestEffort(true)
          .build()));
    }
  }
}
