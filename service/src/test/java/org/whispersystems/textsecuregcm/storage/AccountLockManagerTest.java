package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.util.Collections;
import java.util.List;
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

  private static final String FIRST_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final String SECOND_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("JP"), PhoneNumberUtil.PhoneNumberFormat.E164);

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
  void withLock() throws InterruptedException {
    accountLockManager.withLock(List.of(FIRST_NUMBER, SECOND_NUMBER), () -> {}, executor);

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockTaskThrowsException() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> accountLockManager.withLock(List.of(FIRST_NUMBER, SECOND_NUMBER), () -> {
      throw new RuntimeException();
    }, executor));

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockEmptyList() {
    final Runnable task = mock(Runnable.class);

    assertThrows(IllegalArgumentException.class, () -> accountLockManager.withLock(Collections.emptyList(), () -> {}, executor));
    verify(task, never()).run();
  }

  @Test
  void withLockAsync() throws InterruptedException {
    accountLockManager.withLockAsync(List.of(FIRST_NUMBER, SECOND_NUMBER),
        () -> CompletableFuture.completedFuture(null), executor).join();

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockAsyncTaskThrowsException() throws InterruptedException {
    assertThrows(RuntimeException.class,
        () -> accountLockManager.withLockAsync(List.of(FIRST_NUMBER, SECOND_NUMBER),
            () -> CompletableFuture.failedFuture(new RuntimeException()), executor).join());

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockAsyncEmptyList() {
    final Runnable task = mock(Runnable.class);

    assertThrows(IllegalArgumentException.class,
        () -> accountLockManager.withLockAsync(Collections.emptyList(),
            () -> CompletableFuture.completedFuture(null), executor));

    verify(task, never()).run();
  }
}
