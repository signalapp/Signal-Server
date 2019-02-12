/*
 * Copyright (C) 2019 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AccountCleanerTest {

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final DirectoryQueue  directoryQueue  = mock(DirectoryQueue.class);

  private final Account activeUnexpiredAccount   = mock(Account.class);
  private final Device  activeUnexpiredDevice    = mock(Device.class);
  private final Account activeExpiredAccount     = mock(Account.class);
  private final Device  activeExpiredDevice      = mock(Device.class);
  private final Account inactiveUnexpiredAccount = mock(Account.class);
  private final Device  inactiveUnexpiredDevice  = mock(Device.class);
  private final Account inactiveExpiredAccount   = mock(Account.class);
  private final Device  inactiveExpiredDevice    = mock(Device.class);

  private final Device oldMasterDevice       = mock(Device.class);
  private final Device recentMasterDevice    = mock(Device.class);
  private final Device agingSecondaryDevice  = mock(Device.class);
  private final Device recentSecondaryDevice = mock(Device.class);
  private final Device oldSecondaryDevice    = mock(Device.class);

  private long nowMs;

  @Before
  public void setup() {
    when(activeUnexpiredDevice.isActive()).thenReturn(true);
    when(activeUnexpiredAccount.getLastSeen()).thenReturn(Long.MAX_VALUE);
    when(activeUnexpiredAccount.getMasterDevice()).thenReturn(Optional.of(activeUnexpiredDevice));

    when(activeExpiredAccount.getNumber()).thenReturn(AuthHelper.VALID_NUMBER);
    when(activeExpiredDevice.isActive()).thenReturn(true);
    when(activeExpiredAccount.getLastSeen()).thenReturn(0L);
    when(activeExpiredAccount.getMasterDevice()).thenReturn(Optional.of(activeExpiredDevice));

    when(inactiveUnexpiredDevice.isActive()).thenReturn(false);
    when(inactiveUnexpiredAccount.getLastSeen()).thenReturn(Long.MAX_VALUE);
    when(inactiveUnexpiredAccount.getMasterDevice()).thenReturn(Optional.of(inactiveUnexpiredDevice));

    when(inactiveExpiredDevice.isActive()).thenReturn(false);
    when(inactiveExpiredAccount.getLastSeen()).thenReturn(0L);
    when(inactiveExpiredAccount.getMasterDevice()).thenReturn(Optional.of(inactiveExpiredDevice));

    this.nowMs = System.currentTimeMillis();

    when(oldMasterDevice.getLastSeen()).thenReturn(nowMs - TimeUnit.DAYS.toMillis(366));
    when(oldMasterDevice.isActive()).thenReturn(true);
    when(oldMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(recentMasterDevice.getLastSeen()).thenReturn(nowMs - TimeUnit.DAYS.toMillis(1));
    when(recentMasterDevice.isActive()).thenReturn(true);
    when(recentMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(nowMs - TimeUnit.DAYS.toMillis(31));
    when(agingSecondaryDevice.isActive()).thenReturn(false);
    when(agingSecondaryDevice.getId()).thenReturn(2L);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(nowMs - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.isActive()).thenReturn(true);
    when(recentSecondaryDevice.getId()).thenReturn(2L);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(nowMs - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.isActive()).thenReturn(false);
    when(oldSecondaryDevice.getId()).thenReturn(2L);
  }

  @Test
  public void testUnexpiredAccounts() {
    AccountCleaner accountCleaner = new AccountCleaner(accountsManager, directoryQueue);
    accountCleaner.onCrawlStart();
    accountCleaner.onCrawlChunk(Optional.empty(), Arrays.asList(activeUnexpiredAccount, inactiveUnexpiredAccount, inactiveExpiredAccount));
    accountCleaner.onCrawlEnd(Optional.empty());

    verify(activeUnexpiredDevice, atLeastOnce()).isActive();
    verify(inactiveUnexpiredDevice, atLeastOnce()).isActive();
    verify(inactiveExpiredDevice, atLeastOnce()).isActive();

    verifyNoMoreInteractions(activeUnexpiredDevice);
    verifyNoMoreInteractions(activeExpiredDevice);
    verifyNoMoreInteractions(inactiveUnexpiredDevice);
    verifyNoMoreInteractions(inactiveExpiredDevice);

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(directoryQueue);
  }

  @Test
  public void testExpiredAccounts() {
    AccountCleaner accountCleaner = new AccountCleaner(accountsManager, directoryQueue);
    accountCleaner.onCrawlStart();
    accountCleaner.onCrawlChunk(Optional.empty(), Arrays.asList(activeUnexpiredAccount, activeExpiredAccount, inactiveUnexpiredAccount, inactiveExpiredAccount));
    accountCleaner.onCrawlEnd(Optional.empty());

    verify(activeExpiredDevice).setGcmId(isNull());
    verify(activeExpiredDevice).setApnId(isNull());
    verify(activeExpiredDevice).setFetchesMessages(eq(false));

    verify(accountsManager).update(eq(activeExpiredAccount));
    verify(directoryQueue).deleteRegisteredUser(eq(AuthHelper.VALID_NUMBER));

    verify(activeUnexpiredDevice, atLeastOnce()).isActive();
    verify(activeExpiredDevice, atLeastOnce()).isActive();
    verify(inactiveUnexpiredDevice, atLeastOnce()).isActive();
    verify(inactiveExpiredDevice, atLeastOnce()).isActive();

    verifyNoMoreInteractions(activeUnexpiredDevice);
    verifyNoMoreInteractions(activeExpiredDevice);
    verifyNoMoreInteractions(inactiveUnexpiredDevice);
    verifyNoMoreInteractions(inactiveExpiredDevice);

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(directoryQueue);
  }

  @Test
  public void testIsAccountExpired() {
    Account recentAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(recentMasterDevice);
      add(recentSecondaryDevice);
    }}, "1234".getBytes());

    assertFalse(AccountCleaner.isAccountExpired(recentAccount, nowMs));

    Account oldSecondaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(recentMasterDevice);
      add(agingSecondaryDevice);
    }}, "1234".getBytes());

    assertFalse(AccountCleaner.isAccountExpired(oldSecondaryAccount, nowMs));

    Account agingPrimaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(oldMasterDevice);
      add(agingSecondaryDevice);
    }}, "1234".getBytes());

    assertFalse(AccountCleaner.isAccountExpired(agingPrimaryAccount, nowMs));

    Account oldPrimaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(oldMasterDevice);
      add(oldSecondaryDevice);
    }}, "1234".getBytes());

    assertTrue(AccountCleaner.isAccountExpired(oldPrimaryAccount, nowMs));
  }

}
