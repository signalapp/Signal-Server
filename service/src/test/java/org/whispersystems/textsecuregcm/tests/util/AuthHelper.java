/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import java.security.Principal;
import java.util.Base64;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

public class AuthHelper {
  // Static seed to ensure reproducible tests.
  private static final Random random = new Random(0xf744df3b43a3339cL);

  public static final TestAccount[] TEST_ACCOUNTS = generateTestAccounts();

  public static final String VALID_NUMBER   = "+14150000000";
  public static final UUID   VALID_UUID     = UUID.randomUUID();
  public static final UUID   VALID_PNI      = UUID.randomUUID();
  public static final String VALID_PASSWORD = "foo";

  public static final String VALID_NUMBER_TWO = "+201511111110";
  public static final UUID   VALID_UUID_TWO    = UUID.randomUUID();
  public static final UUID   VALID_PNI_TWO     = UUID.randomUUID();
  public static final String VALID_PASSWORD_TWO = "baz";

  public static final String VALID_NUMBER_3           = "+14445556666";
  public static final UUID   VALID_UUID_3             = UUID.randomUUID();
  public static final UUID   VALID_PNI_3              = UUID.randomUUID();
  public static final String VALID_PASSWORD_3_PRIMARY = "3primary";
  public static final String VALID_PASSWORD_3_LINKED  = "3linked";

  public static final UUID   INVALID_UUID     = UUID.randomUUID();
  public static final String INVALID_PASSWORD = "bar";

  public static final String DISABLED_NUMBER = "+78888888";
  public static final UUID   DISABLED_UUID     = UUID.randomUUID();
  public static final String DISABLED_PASSWORD = "poof";

  public static final String UNDISCOVERABLE_NUMBER   = "+18005551234";
  public static final UUID   UNDISCOVERABLE_UUID     = UUID.randomUUID();
  public static final String UNDISCOVERABLE_PASSWORD = "IT'S A SECRET TO EVERYBODY.";

  public static final String VALID_IDENTITY = "BcxxDU9FGMda70E7+Uvm7pnQcEdXQ64aJCpPUeRSfcFo";

  public static AccountsManager ACCOUNTS_MANAGER       = mock(AccountsManager.class);
  public static Account         VALID_ACCOUNT          = mock(Account.class        );
  public static Account         VALID_ACCOUNT_TWO      = mock(Account.class        );
  public static Account         DISABLED_ACCOUNT       = mock(Account.class        );
  public static Account         UNDISCOVERABLE_ACCOUNT = mock(Account.class        );
  public static Account         VALID_ACCOUNT_3        = mock(Account.class        );

  public static Device VALID_DEVICE           = mock(Device.class);
  public static Device VALID_DEVICE_TWO       = mock(Device.class);
  public static Device DISABLED_DEVICE        = mock(Device.class);
  public static Device UNDISCOVERABLE_DEVICE  = mock(Device.class);
  public static Device VALID_DEVICE_3_PRIMARY = mock(Device.class);
  public static Device VALID_DEVICE_3_LINKED  = mock(Device.class);

  private static AuthenticationCredentials VALID_CREDENTIALS           = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials VALID_CREDENTIALS_TWO       = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials VALID_CREDENTIALS_3_PRIMARY = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials VALID_CREDENTIALS_3_LINKED  = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials DISABLED_CREDENTIALS        = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials UNDISCOVERABLE_CREDENTIALS  = mock(AuthenticationCredentials.class);

  public static PolymorphicAuthDynamicFeature<? extends Principal> getAuthFilter() {
    when(VALID_CREDENTIALS.verify("foo")).thenReturn(true);
    when(VALID_CREDENTIALS_TWO.verify("baz")).thenReturn(true);
    when(VALID_CREDENTIALS_3_PRIMARY.verify(VALID_PASSWORD_3_PRIMARY)).thenReturn(true);
    when(VALID_CREDENTIALS_3_LINKED.verify(VALID_PASSWORD_3_LINKED)).thenReturn(true);
    when(DISABLED_CREDENTIALS.verify(DISABLED_PASSWORD)).thenReturn(true);
    when(UNDISCOVERABLE_CREDENTIALS.verify(UNDISCOVERABLE_PASSWORD)).thenReturn(true);

    when(VALID_DEVICE.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS);
    when(VALID_DEVICE_TWO.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS_TWO);
    when(VALID_DEVICE_3_PRIMARY.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS_3_PRIMARY);
    when(VALID_DEVICE_3_LINKED.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS_3_LINKED);
    when(DISABLED_DEVICE.getAuthenticationCredentials()).thenReturn(DISABLED_CREDENTIALS);
    when(UNDISCOVERABLE_DEVICE.getAuthenticationCredentials()).thenReturn(UNDISCOVERABLE_CREDENTIALS);

    when(VALID_DEVICE.isMaster()).thenReturn(true);
    when(VALID_DEVICE_TWO.isMaster()).thenReturn(true);
    when(DISABLED_DEVICE.isMaster()).thenReturn(true);
    when(UNDISCOVERABLE_DEVICE.isMaster()).thenReturn(true);
    when(VALID_DEVICE_3_PRIMARY.isMaster()).thenReturn(true);
    when(VALID_DEVICE_3_LINKED.isMaster()).thenReturn(false);

    when(VALID_DEVICE.getId()).thenReturn(1L);
    when(VALID_DEVICE_TWO.getId()).thenReturn(1L);
    when(DISABLED_DEVICE.getId()).thenReturn(1L);
    when(UNDISCOVERABLE_DEVICE.getId()).thenReturn(1L);
    when(VALID_DEVICE_3_PRIMARY.getId()).thenReturn(1L);
    when(VALID_DEVICE_3_LINKED.getId()).thenReturn(2L);

    when(VALID_DEVICE.isEnabled()).thenReturn(true);
    when(VALID_DEVICE_TWO.isEnabled()).thenReturn(true);
    when(DISABLED_DEVICE.isEnabled()).thenReturn(false);
    when(UNDISCOVERABLE_DEVICE.isMaster()).thenReturn(true);
    when(VALID_DEVICE_3_PRIMARY.isEnabled()).thenReturn(true);
    when(VALID_DEVICE_3_LINKED.isEnabled()).thenReturn(true);

    when(VALID_ACCOUNT.getDevice(1L)).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT.getMasterDevice()).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT_TWO.getDevice(eq(1L))).thenReturn(Optional.of(VALID_DEVICE_TWO));
    when(VALID_ACCOUNT_TWO.getMasterDevice()).thenReturn(Optional.of(VALID_DEVICE_TWO));
    when(DISABLED_ACCOUNT.getDevice(eq(1L))).thenReturn(Optional.of(DISABLED_DEVICE));
    when(DISABLED_ACCOUNT.getMasterDevice()).thenReturn(Optional.of(DISABLED_DEVICE));
    when(UNDISCOVERABLE_ACCOUNT.getDevice(eq(1L))).thenReturn(Optional.of(UNDISCOVERABLE_DEVICE));
    when(UNDISCOVERABLE_ACCOUNT.getMasterDevice()).thenReturn(Optional.of(UNDISCOVERABLE_DEVICE));
    when(VALID_ACCOUNT_3.getDevice(1L)).thenReturn(Optional.of(VALID_DEVICE_3_PRIMARY));
    when(VALID_ACCOUNT_3.getMasterDevice()).thenReturn(Optional.of(VALID_DEVICE_3_PRIMARY));
    when(VALID_ACCOUNT_3.getDevice(2L)).thenReturn(Optional.of(VALID_DEVICE_3_LINKED));

    when(VALID_ACCOUNT_TWO.getEnabledDeviceCount()).thenReturn(6);

    when(VALID_ACCOUNT.getNumber()).thenReturn(VALID_NUMBER);
    when(VALID_ACCOUNT.getUuid()).thenReturn(VALID_UUID);
    when(VALID_ACCOUNT.getPhoneNumberIdentifier()).thenReturn(VALID_PNI);
    when(VALID_ACCOUNT_TWO.getNumber()).thenReturn(VALID_NUMBER_TWO);
    when(VALID_ACCOUNT_TWO.getUuid()).thenReturn(VALID_UUID_TWO);
    when(VALID_ACCOUNT_TWO.getPhoneNumberIdentifier()).thenReturn(VALID_PNI_TWO);
    when(DISABLED_ACCOUNT.getNumber()).thenReturn(DISABLED_NUMBER);
    when(DISABLED_ACCOUNT.getUuid()).thenReturn(DISABLED_UUID);
    when(UNDISCOVERABLE_ACCOUNT.getNumber()).thenReturn(UNDISCOVERABLE_NUMBER);
    when(UNDISCOVERABLE_ACCOUNT.getUuid()).thenReturn(UNDISCOVERABLE_UUID);
    when(VALID_ACCOUNT_3.getNumber()).thenReturn(VALID_NUMBER_3);
    when(VALID_ACCOUNT_3.getUuid()).thenReturn(VALID_UUID_3);
    when(VALID_ACCOUNT_3.getPhoneNumberIdentifier()).thenReturn(VALID_PNI_3);

    when(VALID_ACCOUNT.isEnabled()).thenReturn(true);
    when(VALID_ACCOUNT_TWO.isEnabled()).thenReturn(true);
    when(DISABLED_ACCOUNT.isEnabled()).thenReturn(false);
    when(UNDISCOVERABLE_ACCOUNT.isEnabled()).thenReturn(true);
    when(VALID_ACCOUNT_3.isEnabled()).thenReturn(true);

    when(VALID_ACCOUNT.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(VALID_ACCOUNT_TWO.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(DISABLED_ACCOUNT.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(UNDISCOVERABLE_ACCOUNT.isDiscoverableByPhoneNumber()).thenReturn(false);
    when(VALID_ACCOUNT_3.isDiscoverableByPhoneNumber()).thenReturn(true);

    when(VALID_ACCOUNT.getIdentityKey()).thenReturn(VALID_IDENTITY);

    reset(ACCOUNTS_MANAGER);

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER)).thenReturn(Optional.of(VALID_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID)).thenReturn(Optional.of(VALID_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI)).thenReturn(Optional.of(VALID_ACCOUNT));

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));

    when(ACCOUNTS_MANAGER.getByE164(DISABLED_NUMBER)).thenReturn(Optional.of(DISABLED_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(DISABLED_UUID)).thenReturn(Optional.of(DISABLED_ACCOUNT));

    when(ACCOUNTS_MANAGER.getByE164(UNDISCOVERABLE_NUMBER)).thenReturn(Optional.of(UNDISCOVERABLE_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(UNDISCOVERABLE_UUID)).thenReturn(Optional.of(UNDISCOVERABLE_ACCOUNT));

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));

    AccountsHelper.setupMockUpdateForAuthHelper(ACCOUNTS_MANAGER);

    for (TestAccount testAccount : TEST_ACCOUNTS) {
      testAccount.setup(ACCOUNTS_MANAGER);
    }

    AuthFilter<BasicCredentials, AuthenticatedAccount> accountAuthFilter = new BasicCredentialAuthFilter.Builder<AuthenticatedAccount>().setAuthenticator(
        new AccountAuthenticator(ACCOUNTS_MANAGER)).buildAuthFilter();
    AuthFilter<BasicCredentials, DisabledPermittedAuthenticatedAccount> disabledPermittedAccountAuthFilter = new BasicCredentialAuthFilter.Builder<DisabledPermittedAuthenticatedAccount>().setAuthenticator(
        new DisabledPermittedAccountAuthenticator(ACCOUNTS_MANAGER)).buildAuthFilter();

    return new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(AuthenticatedAccount.class, accountAuthFilter,
        DisabledPermittedAuthenticatedAccount.class, disabledPermittedAccountAuthFilter));
  }

  public static String getAuthHeader(UUID uuid, long deviceId, String password) {
    return getAuthHeader(uuid.toString() + "." + deviceId, password);
  }

  public static String getAuthHeader(UUID uuid, String password) {
    return getAuthHeader(uuid.toString(), password);
  }

  public static String getProvisioningAuthHeader(String number, String password) {
    return getAuthHeader(number, password);
  }

  private static String getAuthHeader(String identifier, String password) {
    return "Basic " + Base64.getEncoder().encodeToString((identifier + ":" + password).getBytes());
  }

  public static String getUnidentifiedAccessHeader(byte[] key) {
    return Base64.getEncoder().encodeToString(key);
  }

  public static UUID getRandomUUID(Random random) {
    long mostSignificantBits  = random.nextLong();
    long leastSignificantBits = random.nextLong();
    mostSignificantBits  &= 0xffffffffffff0fffL;
    mostSignificantBits  |= 0x0000000000004000L;
    leastSignificantBits &= 0x3fffffffffffffffL;
    leastSignificantBits |= 0x8000000000000000L;
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  public static final class TestAccount {
    public final String                    number;
    public final UUID                      uuid;
    public final String                    password;
    public final Account                   account                   = mock(Account.class);
    public final Device                    device                    = mock(Device.class);
    public final AuthenticationCredentials authenticationCredentials = mock(AuthenticationCredentials.class);

    public TestAccount(String number, UUID uuid, String password) {
      this.number = number;
      this.uuid = uuid;
      this.password = password;
    }

    public String getAuthHeader() {
      return AuthHelper.getAuthHeader(uuid, password);
    }

    private void setup(final AccountsManager accountsManager) {
      when(authenticationCredentials.verify(password)).thenReturn(true);
      when(device.getAuthenticationCredentials()).thenReturn(authenticationCredentials);
      when(device.isMaster()).thenReturn(true);
      when(device.getId()).thenReturn(1L);
      when(device.isEnabled()).thenReturn(true);
      when(account.getDevice(1L)).thenReturn(Optional.of(device));
      when(account.getMasterDevice()).thenReturn(Optional.of(device));
      when(account.getNumber()).thenReturn(number);
      when(account.getUuid()).thenReturn(uuid);
      when(account.isEnabled()).thenReturn(true);
      when(accountsManager.getByE164(number)).thenReturn(Optional.of(account));
      when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    }
  }

  private static TestAccount[] generateTestAccounts() {
    final TestAccount[] testAccounts = new TestAccount[20];
    final long numberBase = 1_409_000_0000L;
    for (int i = 0; i < testAccounts.length; i++) {
      long currentNumber = numberBase + i;
      testAccounts[i] = new TestAccount("+" + currentNumber, getRandomUUID(random), "TestAccountPassword-" + currentNumber);
    }
    return testAccounts;
  }
}
