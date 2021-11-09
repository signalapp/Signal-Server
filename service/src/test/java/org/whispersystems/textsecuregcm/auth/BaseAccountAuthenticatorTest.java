/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import io.dropwizard.auth.basic.BasicCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Pair;

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

    acct1 = new Account("+14088675309", AuthHelper.getRandomUUID(random), UUID.randomUUID(),
        Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
    acct2 = new Account("+14098675309", AuthHelper.getRandomUUID(random), UUID.randomUUID(),
        Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, yesterday, 0, null, 0, null)), null);
    oldAccount = new Account("+14108675309", AuthHelper.getRandomUUID(random), UUID.randomUUID(),
        Set.of(new Device(1, null, null, null,
        null, null, null, false, 0, null, oldTime, 0, null, 0, null)), null);

    AccountsHelper.setupMockUpdate(accountsManager);
  }

  @Test
  void testUpdateLastSeenMiddleOfDay() {
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
  void testUpdateLastSeenStartOfDay() {
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
  void testUpdateLastSeenEndOfDay() {
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
  void testNeverWriteYesterday() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(today));

    final Device device = oldAccount.getDevices().stream().findFirst().get();

    baseAccountAuthenticator.updateLastSeen(oldAccount, device);

    verify(accountsManager).updateDeviceLastSeen(eq(oldAccount), eq(device), anyLong());

    assertThat(device.getLastSeen()).isEqualTo(today);
  }

  @Test
  void testAuthenticate() {
    final UUID uuid = UUID.randomUUID();
    final long deviceId = 1;
    final String password = "12345";

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(clock.instant()).thenReturn(Instant.now());
    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    when(account.getUuid()).thenReturn(uuid);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(account.isEnabled()).thenReturn(true);
    when(device.getId()).thenReturn(deviceId);
    when(device.isEnabled()).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(credentials.verify(password)).thenReturn(true);

    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
        baseAccountAuthenticator.authenticate(new BasicCredentials(uuid.toString(), password), true);

    assertThat(maybeAuthenticatedAccount).isPresent();
    assertThat(maybeAuthenticatedAccount.get().getAccount().getUuid()).isEqualTo(uuid);
    assertThat(maybeAuthenticatedAccount.get().getAuthenticatedDevice()).isEqualTo(device);
  }

  @Test
  void testAuthenticateNonDefaultDevice() {
    final UUID uuid = UUID.randomUUID();
    final long deviceId = 2;
    final String password = "12345";

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(clock.instant()).thenReturn(Instant.now());
    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    when(account.getUuid()).thenReturn(uuid);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(account.isEnabled()).thenReturn(true);
    when(device.getId()).thenReturn(deviceId);
    when(device.isEnabled()).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(credentials.verify(password)).thenReturn(true);

    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
        baseAccountAuthenticator.authenticate(new BasicCredentials(uuid + "." + deviceId, password), true);

    assertThat(maybeAuthenticatedAccount).isPresent();
    assertThat(maybeAuthenticatedAccount.get().getAccount().getUuid()).isEqualTo(uuid);
    assertThat(maybeAuthenticatedAccount.get().getAuthenticatedDevice()).isEqualTo(device);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAuthenticateEnabledRequired(final boolean enabledRequired) {
    final UUID uuid = UUID.randomUUID();
    final long deviceId = 1;
    final String password = "12345";

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(clock.instant()).thenReturn(Instant.now());
    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    when(account.getUuid()).thenReturn(uuid);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(account.isEnabled()).thenReturn(false);
    when(device.getId()).thenReturn(deviceId);
    when(device.isEnabled()).thenReturn(false);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(credentials.verify(password)).thenReturn(true);

    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
        baseAccountAuthenticator.authenticate(new BasicCredentials(uuid.toString(), password), enabledRequired);

    if (enabledRequired) {
      assertThat(maybeAuthenticatedAccount).isEmpty();
    } else {
      assertThat(maybeAuthenticatedAccount).isPresent();
      assertThat(maybeAuthenticatedAccount.get().getAccount().getUuid()).isEqualTo(uuid);
      assertThat(maybeAuthenticatedAccount.get().getAuthenticatedDevice()).isEqualTo(device);
    }
  }

  @Test
  void testAuthenticateAccountNotFound() {
    assertThat(baseAccountAuthenticator.authenticate(new BasicCredentials(UUID.randomUUID().toString(), "password"), true))
        .isEmpty();
  }

  @Test
  void testAuthenticateDeviceNotFound() {
    final UUID uuid = UUID.randomUUID();
    final long deviceId = 1;
    final String password = "12345";

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(clock.instant()).thenReturn(Instant.now());
    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    when(account.getUuid()).thenReturn(uuid);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(account.isEnabled()).thenReturn(true);
    when(device.getId()).thenReturn(deviceId);
    when(device.isEnabled()).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(credentials.verify(password)).thenReturn(true);

    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
        baseAccountAuthenticator.authenticate(new BasicCredentials(uuid + "." + (deviceId + 1), password), true);

    assertThat(maybeAuthenticatedAccount).isEmpty();
    verify(account).getDevice(deviceId + 1);
  }

  @Test
  void testAuthenticateIncorrectPassword() {
    final UUID uuid = UUID.randomUUID();
    final long deviceId = 1;
    final String password = "12345";

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(clock.instant()).thenReturn(Instant.now());
    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    when(account.getUuid()).thenReturn(uuid);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(account.isEnabled()).thenReturn(true);
    when(device.getId()).thenReturn(deviceId);
    when(device.isEnabled()).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(credentials.verify(password)).thenReturn(true);

    final String incorrectPassword = password + "incorrect";

    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount =
        baseAccountAuthenticator.authenticate(new BasicCredentials(uuid.toString(), incorrectPassword), true);

    assertThat(maybeAuthenticatedAccount).isEmpty();
    verify(credentials).verify(incorrectPassword);
  }

  @ParameterizedTest
  @MethodSource
  void testAuthenticateMalformedCredentials(final String username) {
    final Optional<AuthenticatedAccount> maybeAuthenticatedAccount = assertDoesNotThrow(
        () -> baseAccountAuthenticator.authenticate(new BasicCredentials(username, "password"), true));

    assertThat(maybeAuthenticatedAccount).isEmpty();
    verify(accountsManager, never()).getByAccountIdentifier(any(UUID.class));
  }

  private static Stream<String> testAuthenticateMalformedCredentials() {
    return Stream.of(
        "",
        ".4",
        "This is definitely not a valid UUID",
        UUID.randomUUID() + ".");
  }

  @ParameterizedTest
  @MethodSource
  void testGetIdentifierAndDeviceId(final String username, final String expectedIdentifier, final long expectedDeviceId) {
    final Pair<String, Long> identifierAndDeviceId = BaseAccountAuthenticator.getIdentifierAndDeviceId(username);

    assertEquals(expectedIdentifier, identifierAndDeviceId.first());
    assertEquals(expectedDeviceId, identifierAndDeviceId.second());
  }

  private static Stream<Arguments> testGetIdentifierAndDeviceId() {
    return Stream.of(
        Arguments.of("", "", Device.MASTER_ID),
        Arguments.of("test", "test", Device.MASTER_ID),
        Arguments.of("test.7", "test", 7));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      ".",
      ".....",
      "test.7.8",
      "test."
  })
  void testGetIdentifierAndDeviceIdMalformed(final String malformedUsername) {
    assertThrows(IllegalArgumentException.class,
        () -> BaseAccountAuthenticator.getIdentifierAndDeviceId(malformedUsername));
  }
}
