/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.AccountsHelper.eqUuid;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.Util;

class PushFeedbackProcessorTest {

  private AccountsManager accountsManager = mock(AccountsManager.class);

  private Account uninstalledAccount    = mock(Account.class);
  private Account mixedAccount          = mock(Account.class);
  private Account freshAccount          = mock(Account.class);
  private Account cleanAccount          = mock(Account.class);
  private Account stillActiveAccount    = mock(Account.class);
  private Account undiscoverableAccount = mock(Account.class);

  private Device uninstalledDevice       = mock(Device.class);
  private Device uninstalledDeviceTwo    = mock(Device.class);
  private Device installedDevice         = mock(Device.class);
  private Device installedDeviceTwo      = mock(Device.class);
  private Device recentUninstalledDevice = mock(Device.class);
  private Device stillActiveDevice       = mock(Device.class);
  private Device undiscoverableDevice    = mock(Device.class);

  @BeforeEach
  void setup() {
    AccountsHelper.setupMockUpdate(accountsManager);

    when(uninstalledDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(uninstalledDevice.getLastSeen()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(uninstalledDeviceTwo.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(3));
    when(uninstalledDeviceTwo.getLastSeen()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(3));

    when(installedDevice.getUninstalledFeedbackTimestamp()).thenReturn(0L);
    when(installedDeviceTwo.getUninstalledFeedbackTimestamp()).thenReturn(0L);

    when(recentUninstalledDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentUninstalledDevice.getLastSeen()).thenReturn(Util.todayInMillis());

    when(stillActiveDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(stillActiveDevice.getLastSeen()).thenReturn(Util.todayInMillis());

    when(undiscoverableDevice.getUninstalledFeedbackTimestamp()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));
    when(undiscoverableDevice.getLastSeen()).thenReturn(Util.todayInMillis() - TimeUnit.DAYS.toMillis(2));

    when(uninstalledAccount.getDevices()).thenReturn(Set.of(uninstalledDevice));
    when(mixedAccount.getDevices()).thenReturn(Set.of(installedDevice, uninstalledDeviceTwo));
    when(freshAccount.getDevices()).thenReturn(Set.of(recentUninstalledDevice));
    when(cleanAccount.getDevices()).thenReturn(Set.of(installedDeviceTwo));
    when(stillActiveAccount.getDevices()).thenReturn(Set.of(stillActiveDevice));
    when(undiscoverableAccount.getDevices()).thenReturn(Set.of(undiscoverableDevice));

    when(mixedAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(freshAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(cleanAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(stillActiveAccount.getUuid()).thenReturn(UUID.randomUUID());

    when(uninstalledAccount.isEnabled()).thenReturn(true);
    when(uninstalledAccount.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(uninstalledAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(uninstalledAccount.getNumber()).thenReturn("+18005551234");

    when(undiscoverableAccount.isEnabled()).thenReturn(true);
    when(undiscoverableAccount.isDiscoverableByPhoneNumber()).thenReturn(false);
    when(undiscoverableAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(undiscoverableAccount.getNumber()).thenReturn("+18005559876");

    AccountsHelper.setupMockGet(accountsManager, Set.of(uninstalledAccount, mixedAccount, freshAccount, cleanAccount, stillActiveAccount, undiscoverableAccount));
  }


  @Test
  void testEmpty() throws AccountDatabaseCrawlerRestartException {
    PushFeedbackProcessor processor = new PushFeedbackProcessor(accountsManager);
    processor.timeAndProcessCrawlChunk(Optional.of(UUID.randomUUID()), Collections.emptyList());

    verifyNoInteractions(accountsManager);
  }

  @Test
  void testUpdate() throws AccountDatabaseCrawlerRestartException {
    PushFeedbackProcessor processor = new PushFeedbackProcessor(accountsManager);
    processor.timeAndProcessCrawlChunk(Optional.of(UUID.randomUUID()), List.of(uninstalledAccount, mixedAccount, stillActiveAccount, freshAccount, cleanAccount, undiscoverableAccount));

    verify(uninstalledDevice).setApnId(isNull());
    verify(uninstalledDevice).setGcmId(isNull());
    verify(uninstalledDevice).setFetchesMessages(eq(false));

    verify(accountsManager).update(eqUuid(uninstalledAccount), any());

    verify(uninstalledDeviceTwo).setApnId(isNull());
    verify(uninstalledDeviceTwo).setGcmId(isNull());
    verify(uninstalledDeviceTwo).setFetchesMessages(eq(false));

    verify(installedDevice, never()).setApnId(any());
    verify(installedDevice, never()).setGcmId(any());
    verify(installedDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager).update(eqUuid(mixedAccount), any());

    verify(recentUninstalledDevice, never()).setApnId(any());
    verify(recentUninstalledDevice, never()).setGcmId(any());
    verify(recentUninstalledDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager, never()).update(eqUuid(freshAccount), any());

    verify(installedDeviceTwo, never()).setApnId(any());
    verify(installedDeviceTwo, never()).setGcmId(any());
    verify(installedDeviceTwo, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager, never()).update(eqUuid(cleanAccount), any());

    verify(stillActiveDevice).setUninstalledFeedbackTimestamp(eq(0L));
    verify(stillActiveDevice, never()).setApnId(any());
    verify(stillActiveDevice, never()).setGcmId(any());
    verify(stillActiveDevice, never()).setFetchesMessages(anyBoolean());

    verify(accountsManager).update(eqUuid(stillActiveAccount), any());
  }

}
