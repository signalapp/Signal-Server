/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.AccountsManager.DeletionReason;

class AccountCleanerTest {

  private final AccountsManager accountsManager = mock(AccountsManager.class);

  private final Account deletedDisabledAccount   = mock(Account.class);
  private final Account undeletedDisabledAccount = mock(Account.class);
  private final Account undeletedEnabledAccount  = mock(Account.class);

  private final Device  deletedDisabledDevice    = mock(Device.class );
  private final Device  undeletedDisabledDevice  = mock(Device.class );
  private final Device  undeletedEnabledDevice   = mock(Device.class );

  private ExecutorService deletionExecutor;


  @BeforeEach
  void setup() {
    when(deletedDisabledDevice.isEnabled()).thenReturn(false);
    when(deletedDisabledDevice.getGcmId()).thenReturn(null);
    when(deletedDisabledDevice.getApnId()).thenReturn(null);
    when(deletedDisabledDevice.getVoipApnId()).thenReturn(null);
    when(deletedDisabledDevice.getFetchesMessages()).thenReturn(false);
    when(deletedDisabledAccount.isEnabled()).thenReturn(false);
    when(deletedDisabledAccount.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1000));
    when(deletedDisabledAccount.getMasterDevice()).thenReturn(Optional.of(deletedDisabledDevice));
    when(deletedDisabledAccount.getNumber()).thenReturn("+14151231234");
    when(deletedDisabledAccount.getUuid()).thenReturn(UUID.randomUUID());

    when(undeletedDisabledDevice.isEnabled()).thenReturn(false);
    when(undeletedDisabledDevice.getGcmId()).thenReturn("foo");
    when(undeletedDisabledAccount.isEnabled()).thenReturn(false);
    when(undeletedDisabledAccount.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(undeletedDisabledAccount.getMasterDevice()).thenReturn(Optional.of(undeletedDisabledDevice));
    when(undeletedDisabledAccount.getNumber()).thenReturn("+14152222222");
    when(undeletedDisabledAccount.getUuid()).thenReturn(UUID.randomUUID());

    when(undeletedEnabledDevice.isEnabled()).thenReturn(true);
    when(undeletedEnabledDevice.getApnId()).thenReturn("bar");
    when(undeletedEnabledAccount.isEnabled()).thenReturn(true);
    when(undeletedEnabledAccount.getMasterDevice()).thenReturn(Optional.of(undeletedEnabledDevice));
    when(undeletedEnabledAccount.getNumber()).thenReturn("+14153333333");
    when(undeletedEnabledAccount.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(364));
    when(undeletedEnabledAccount.getUuid()).thenReturn(UUID.randomUUID());

    deletionExecutor = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    deletionExecutor.shutdown();
    deletionExecutor.awaitTermination(2, TimeUnit.SECONDS);
  }

  @Test
  void testAccounts() throws AccountDatabaseCrawlerRestartException, InterruptedException {
    AccountCleaner accountCleaner = new AccountCleaner(accountsManager, deletionExecutor);
    accountCleaner.onCrawlStart();
    accountCleaner.timeAndProcessCrawlChunk(Optional.empty(), Arrays.asList(deletedDisabledAccount, undeletedDisabledAccount, undeletedEnabledAccount));
    accountCleaner.onCrawlEnd(Optional.empty());

    verify(accountsManager).delete(deletedDisabledAccount, DeletionReason.EXPIRED);
    verify(accountsManager).delete(undeletedDisabledAccount, DeletionReason.EXPIRED);
    verify(accountsManager, never()).delete(eq(undeletedEnabledAccount), any());

    verifyNoMoreInteractions(accountsManager);
  }

}
