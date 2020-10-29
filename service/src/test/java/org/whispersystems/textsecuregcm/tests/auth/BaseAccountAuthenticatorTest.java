/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.BaseAccountAuthenticator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import java.time.Clock;
import java.time.Instant;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BaseAccountAuthenticatorTest {

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

    @Before
    public void setup() {
        accountsManager            = mock(AccountsManager.class);
        clock                      = mock(Clock.class);
        baseAccountAuthenticator   = new BaseAccountAuthenticator(accountsManager, clock);

        acct1      = new Account("+14088675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null, null, null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
        acct2      = new Account("+14098675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null, null, null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
        oldAccount = new Account("+14108675309", AuthHelper.getRandomUUID(random), Set.of(new Device(1, null, null, null, null, null, null, null, false, 0, null, oldTime, 0, null, 0, null)), null);
    }

    @Test
    public void testUpdateLastSeenMiddleOfDay() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(currentTime));

        baseAccountAuthenticator.updateLastSeen(acct1, acct1.getDevices().stream().findFirst().get());
        baseAccountAuthenticator.updateLastSeen(acct2, acct2.getDevices().stream().findFirst().get());

        verify(accountsManager, never()).update(acct1);
        verify(accountsManager).update(acct2);

        assertThat(acct1.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(yesterday);
        assertThat(acct2.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(today);
    }

    @Test
    public void testUpdateLastSeenStartOfDay() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(today));

        baseAccountAuthenticator.updateLastSeen(acct1, acct1.getDevices().stream().findFirst().get());
        baseAccountAuthenticator.updateLastSeen(acct2, acct2.getDevices().stream().findFirst().get());

        verify(accountsManager, never()).update(acct1);
        verify(accountsManager, never()).update(acct2);

        assertThat(acct1.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(yesterday);
        assertThat(acct2.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(yesterday);
    }

    @Test
    public void testUpdateLastSeenEndOfDay() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(today + 86_400_000L - 1));

        baseAccountAuthenticator.updateLastSeen(acct1, acct1.getDevices().stream().findFirst().get());
        baseAccountAuthenticator.updateLastSeen(acct2, acct2.getDevices().stream().findFirst().get());

        verify(accountsManager).update(acct1);
        verify(accountsManager).update(acct2);

        assertThat(acct1.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(today);
        assertThat(acct2.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(today);
    }

    @Test
    public void testNeverWriteYesterday() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(today));

        baseAccountAuthenticator.updateLastSeen(oldAccount, oldAccount.getDevices().stream().findFirst().get());

        verify(accountsManager).update(oldAccount);

        assertThat(oldAccount.getDevices().stream().findFirst().get().getLastSeen()).isEqualTo(today);
    }
}
