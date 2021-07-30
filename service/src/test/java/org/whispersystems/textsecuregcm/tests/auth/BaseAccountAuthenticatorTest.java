/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.BaseAccountAuthenticator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

class BaseAccountAuthenticatorTest {

  private final Random             random       = new Random(867_5309L);
  private final long               today        = 1590451200000L;
  private final long               yesterday    = today - 86_400_000L;
  private final long               oldTime      = yesterday - 86_400_000L;
  private final long               currentTime  = today + 68_000_000L;

  private AccountsManager          accountsManager;
  private BaseAccountAuthenticator baseAccountAuthenticator;
  private Clock                    clock;
  private Account                  acct1;
  private Account                  acct2;
  private Account                  oldAccount;

  @BeforeEach
  void setup() {
    accountsManager = mock(AccountsManager.class);
    clock = mock(Clock.class);
    baseAccountAuthenticator = new BaseAccountAuthenticator(accountsManager, clock);

    acct1 = new Account("+14088675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
    acct2 = new Account("+14098675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
    oldAccount = new Account("+14108675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, oldTime, 0, null, 0, null)), null);

    AccountsHelper.setupMockUpdate(accountsManager);
  }

  @Test
  void  testUpdateLastSeenMiddleOfDay() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(currentTime));

    final Device device1 = acct1.getDevices().stream().findFirst().get();
    final Device device2 = acct2.getDevices().stream().findFirst().get();

    final Account updatedAcct1 = baseAccountAuthenticator.updateLastSeen(acct1, device1);
    final Account updatedAcct2 = baseAccountAuthenticator.updateLastSeen(acct2, device2);

    verify(accountsManager, never()).updateDeviceLastSeen(eq(acct1), any(), anyLong());
    verify(accountsManager).updateDeviceLastSeen(eq(acct2), eq(device2), anyLong());

    assertThat(device1.getLastSeen()).isEqualTo(yesterday);
    assertThat(device2.getLastSeen()).isEqualTo(today);

    assertThat(acct1).isSameAs(updatedAcct1);
    assertThat(acct2).isNotSameAs(updatedAcct2);
  }

  @Test
  void  testUpdateLastSeenStartOfDay() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(today));

    final Device device1 = acct1.getDevices().stream().findFirst().get();
    final Device device2 = acct2.getDevices().stream().findFirst().get();

    final Account updatedAcct1 = baseAccountAuthenticator.updateLastSeen(acct1, device1);
    final Account updatedAcct2 = baseAccountAuthenticator.updateLastSeen(acct2, device2);

    verify(accountsManager, never()).updateDeviceLastSeen(eq(acct1), any(), anyLong());
    verify(accountsManager, never()).updateDeviceLastSeen(eq(acct2), any(), anyLong());

    assertThat(device1.getLastSeen()).isEqualTo(yesterday);
    assertThat(device2.getLastSeen()).isEqualTo(yesterday);

    assertThat(acct1).isSameAs(updatedAcct1);
    assertThat(acct2).isSameAs(updatedAcct2);
  }

  @Test
  void  testUpdateLastSeenEndOfDay() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(today + 86_400_000L - 1));

    final Device device1 = acct1.getDevices().stream().findFirst().get();
    final Device device2 = acct2.getDevices().stream().findFirst().get();

    final Account updatedAcct1 = baseAccountAuthenticator.updateLastSeen(acct1, device1);
    final Account updatedAcct2 = baseAccountAuthenticator.updateLastSeen(acct2, device2);

    verify(accountsManager).updateDeviceLastSeen(eq(acct1), eq(device1), anyLong());
    verify(accountsManager).updateDeviceLastSeen(eq(acct2), eq(device2), anyLong());

    assertThat(device1.getLastSeen()).isEqualTo(today);
    assertThat(device2.getLastSeen()).isEqualTo(today);

    assertThat(updatedAcct1).isNotSameAs(acct1);
    assertThat(updatedAcct2).isNotSameAs(acct2);
  }

  @Test
  void  testNeverWriteYesterday() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(today));

    final Device device = oldAccount.getDevices().stream().findFirst().get();

    baseAccountAuthenticator.updateLastSeen(oldAccount, device);

    verify(accountsManager).updateDeviceLastSeen(eq(oldAccount), eq(device), anyLong());

    assertThat(device.getLastSeen()).isEqualTo(today);
  }
}
