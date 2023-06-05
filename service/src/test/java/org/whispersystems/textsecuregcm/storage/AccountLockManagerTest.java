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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountLockManagerTest {

  private AmazonDynamoDBLockClient lockClient;

  private AccountLockManager accountLockManager;

  private static final String FIRST_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final String SECOND_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("JP"), PhoneNumberUtil.PhoneNumberFormat.E164);

  @BeforeEach
  void setUp() {
    lockClient = mock(AmazonDynamoDBLockClient.class);

    accountLockManager = new AccountLockManager(lockClient);
  }

  @Test
  void withLock() throws InterruptedException {
    accountLockManager.withLock(List.of(FIRST_NUMBER, SECOND_NUMBER), () -> {});

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockTaskThrowsException() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> accountLockManager.withLock(List.of(FIRST_NUMBER, SECOND_NUMBER), () -> {
      throw new RuntimeException();
    }));

    verify(lockClient, times(2)).acquireLock(any());
    verify(lockClient, times(2)).releaseLock(any(ReleaseLockOptions.class));
  }

  @Test
  void withLockEmptyList() {
    final Runnable task = mock(Runnable.class);

    assertThrows(IllegalArgumentException.class, () -> accountLockManager.withLock(Collections.emptyList(), () -> {}));
    verify(task, never()).run();
  }
}
