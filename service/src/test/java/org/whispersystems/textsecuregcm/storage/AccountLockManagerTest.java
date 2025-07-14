package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountLockManagerTest {

  private AmazonDynamoDBLockClient lockClient;
  private ExecutorService executor;

  private AccountLockManager accountLockManager;

  private static final UUID FIRST_PNI = UUID.randomUUID();
  private static final UUID SECOND_PNI = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    lockClient = mock(AmazonDynamoDBLockClient.class);
    executor = Executors.newSingleThreadExecutor();

    accountLockManager = new AccountLockManager(lockClient);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void withLock() throws Exception {
    accountLockManager.withLock(Set.of(FIRST_PNI, SECOND_PNI), () -> null, executor);

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockTaskThrowsException() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> accountLockManager.withLock(Set.of(FIRST_PNI, SECOND_PNI), () -> {
          throw new RuntimeException();
    }, executor));

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockEmptyList() {
    final Runnable task = mock(Runnable.class);

    assertThrows(IllegalArgumentException.class, () -> accountLockManager.withLock(Collections.emptySet(), () -> null,
        executor));
    verify(task, never()).run();
  }

  @Test
  void withLockAsync() throws InterruptedException {
    accountLockManager.withLockAsync(
        Set.of(FIRST_PNI, SECOND_PNI), () -> CompletableFuture.completedFuture(null), executor).join();

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockAsyncTaskThrowsException() throws InterruptedException {
    assertThrows(RuntimeException.class,
        () -> accountLockManager.withLockAsync(
                Set.of(FIRST_PNI, SECOND_PNI), () -> CompletableFuture.failedFuture(new RuntimeException()), executor)
            .join());

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockAsyncEmptyList() {
    final Runnable task = mock(Runnable.class);

    assertThrows(IllegalArgumentException.class,
        () -> accountLockManager.withLockAsync(Collections.emptySet(), () -> CompletableFuture.completedFuture(null),
            executor));

    verify(task, never()).run();
  }
}
