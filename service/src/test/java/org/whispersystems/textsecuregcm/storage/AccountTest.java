/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.DevicesHelper.createDevice;
import static org.whispersystems.textsecuregcm.tests.util.DevicesHelper.setEnabled;

import com.fasterxml.jackson.annotation.JsonFilter;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

class AccountTest {

  private final Device oldPrimaryDevice = mock(Device.class);
  private final Device recentPrimaryDevice = mock(Device.class);
  private final Device agingSecondaryDevice = mock(Device.class);
  private final Device recentSecondaryDevice = mock(Device.class);
  private final Device oldSecondaryDevice = mock(Device.class);

  private final Device paymentActivationCapableDevice = mock(Device.class);
  private final Device paymentActivationIncapableDevice = mock(Device.class);
  private final Device paymentActivationIncapableExpiredDevice = mock(Device.class);

  @BeforeEach
  void setup() {
    when(oldPrimaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldPrimaryDevice.isEnabled()).thenReturn(true);
    when(oldPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    when(recentPrimaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentPrimaryDevice.isEnabled()).thenReturn(true);
    when(recentPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(agingSecondaryDevice.isEnabled()).thenReturn(false);
    final byte deviceId2 = 2;
    when(agingSecondaryDevice.getId()).thenReturn(deviceId2);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.isEnabled()).thenReturn(true);
    when(recentSecondaryDevice.getId()).thenReturn(deviceId2);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.isEnabled()).thenReturn(false);
    when(oldSecondaryDevice.getId()).thenReturn(deviceId2);

    when(paymentActivationCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true));
    when(paymentActivationCapableDevice.isEnabled()).thenReturn(true);
    when(paymentActivationIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, false));
    when(paymentActivationIncapableDevice.isEnabled()).thenReturn(true);
    when(paymentActivationIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, false));
    when(paymentActivationIncapableExpiredDevice.isEnabled()).thenReturn(false);

  }

  @Test
  void testIsEnabled() {
    final Device enabledPrimaryDevice = mock(Device.class);
    final Device enabledLinkedDevice = mock(Device.class);
    final Device disabledPrimaryDevice = mock(Device.class);
    final Device disabledLinkedDevice = mock(Device.class);

    when(enabledPrimaryDevice.isEnabled()).thenReturn(true);
    when(enabledLinkedDevice.isEnabled()).thenReturn(true);
    when(disabledPrimaryDevice.isEnabled()).thenReturn(false);
    when(disabledLinkedDevice.isEnabled()).thenReturn(false);

    when(enabledPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    final byte deviceId2 = 2;
    when(enabledLinkedDevice.getId()).thenReturn(deviceId2);
    when(disabledPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(disabledLinkedDevice.getId()).thenReturn(deviceId2);

    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledPrimaryDevice)).isEnabled());
    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledPrimaryDevice, enabledLinkedDevice)).isEnabled());
    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledPrimaryDevice, disabledLinkedDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledPrimaryDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledPrimaryDevice, enabledLinkedDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledPrimaryDevice, disabledLinkedDevice)).isEnabled());
  }

  @Test
  void testIsTransferSupported() {
    final Device transferCapablePrimaryDevice = mock(Device.class);
    final Device nonTransferCapablePrimaryDevice = mock(Device.class);
    final Device transferCapableLinkedDevice = mock(Device.class);

    final DeviceCapabilities transferCapabilities = mock(DeviceCapabilities.class);
    final DeviceCapabilities nonTransferCapabilities = mock(DeviceCapabilities.class);

    when(transferCapablePrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(transferCapablePrimaryDevice.isPrimary()).thenReturn(true);
    when(transferCapablePrimaryDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(nonTransferCapablePrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(nonTransferCapablePrimaryDevice.isPrimary()).thenReturn(true);
    when(nonTransferCapablePrimaryDevice.getCapabilities()).thenReturn(nonTransferCapabilities);

    when(transferCapableLinkedDevice.getId()).thenReturn((byte) 2);
    when(transferCapableLinkedDevice.isPrimary()).thenReturn(false);
    when(transferCapableLinkedDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(transferCapabilities.transfer()).thenReturn(true);
    when(nonTransferCapabilities.transfer()).thenReturn(false);

    {
      final Account transferablePrimaryAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(transferCapablePrimaryDevice), "1234".getBytes());

      assertTrue(transferablePrimaryAccount.isTransferSupported());
    }

    {
      final Account nonTransferablePrimaryAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapablePrimaryDevice), "1234".getBytes());

      assertFalse(nonTransferablePrimaryAccount.isTransferSupported());
    }

    {
      final Account transferableLinkedAccount = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapablePrimaryDevice, transferCapableLinkedDevice), "1234".getBytes());

      assertFalse(transferableLinkedAccount.isTransferSupported());
    }
  }

  @Test
  void testDiscoverableByPhoneNumber() {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(recentPrimaryDevice),
        "1234".getBytes());

    assertTrue(account.isDiscoverableByPhoneNumber(),
        "Freshly-loaded legacy accounts should be discoverable by phone number.");

    account.setDiscoverableByPhoneNumber(false);
    assertFalse(account.isDiscoverableByPhoneNumber());

    account.setDiscoverableByPhoneNumber(true);
    assertTrue(account.isDiscoverableByPhoneNumber());
  }

  @Test
  void isPaymentActivationSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(paymentActivationCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPaymentActivationSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(paymentActivationCapableDevice, paymentActivationIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPaymentActivationSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(paymentActivationCapableDevice, paymentActivationIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPaymentActivationSupported()).isTrue();
  }

  @Test
  void stale() {
    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), Collections.emptyList(),
        new byte[0]);

    assertDoesNotThrow(account::getNumber);

    account.markStale();

    assertThrows(AssertionError.class, account::getNumber);
    assertDoesNotThrow(account::getUuid);
  }

  @Test
  void getNextDeviceId() {

    final List<Device> devices = List.of(createDevice(Device.PRIMARY_ID));

    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), devices, new byte[0]);

    final byte deviceId2 = 2;
    assertThat(account.getNextDeviceId()).isEqualTo(deviceId2);

    account.addDevice(createDevice(deviceId2));

    final byte deviceId3 = 3;
    assertThat(account.getNextDeviceId()).isEqualTo(deviceId3);

    account.addDevice(createDevice(deviceId3));

    setEnabled(account.getDevice(deviceId2).orElseThrow(), false);

    assertThat(account.getNextDeviceId()).isEqualTo((byte) 4);

    account.removeDevice(deviceId2);

    assertThat(account.getNextDeviceId()).isEqualTo(deviceId2);

    while (account.getNextDeviceId() < Device.MAXIMUM_DEVICE_ID) {
      account.addDevice(createDevice(account.getNextDeviceId()));
    }

    account.addDevice(createDevice(Device.MAXIMUM_DEVICE_ID));

    assertThatThrownBy(account::getNextDeviceId).isInstanceOf(RuntimeException.class);
  }

  @Test
  void replaceDevice() {
    final Device firstDevice = createDevice(Device.PRIMARY_ID);
    final Device secondDevice = createDevice(Device.PRIMARY_ID);
    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), List.of(firstDevice), new byte[0]);

    assertEquals(List.of(firstDevice), account.getDevices());

    account.addDevice(secondDevice);

    assertEquals(List.of(secondDevice), account.getDevices());
  }

  @Test
  void addAndRemoveBadges() {
    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), List.of(createDevice(Device.PRIMARY_ID)), new byte[0]);
    final Clock clock = TestClock.pinned(Instant.ofEpochSecond(40));

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(42), false));
    account.addBadge(clock, new AccountBadge("bar", Instant.ofEpochSecond(44), true));
    account.addBadge(clock, new AccountBadge("baz", Instant.ofEpochSecond(46), true));

    assertThat(account.getBadges()).hasSize(3);

    account.removeBadge(clock, "baz");

    assertThat(account.getBadges()).hasSize(2);

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(50), false));

    assertThat(account.getBadges()).hasSize(2).element(0).satisfies(badge -> {
      assertThat(badge.getId()).isEqualTo("foo");
      assertThat(badge.getExpiration().getEpochSecond()).isEqualTo(50);
      assertThat(badge.isVisible()).isFalse();
    });

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(51), true));

    assertThat(account.getBadges()).hasSize(2).element(0).satisfies(badge -> {
      assertThat(badge.getId()).isEqualTo("foo");
      assertThat(badge.getExpiration().getEpochSecond()).isEqualTo(51);
      assertThat(badge.isVisible()).isTrue();
    });
  }

  @Test
  public void testAccountClassJsonFilterIdMatchesClassName() throws Exception {
    // Some logic relies on the @JsonFilter name being equal to the class name.
    // This test is just making sure that annotation is there and that the ID matches class name.
    final Optional<Annotation> maybeJsonFilterAnnotation = Arrays.stream(Account.class.getAnnotations())
        .filter(a -> a.annotationType().equals(JsonFilter.class))
        .findFirst();
    assertTrue(maybeJsonFilterAnnotation.isPresent());
    final JsonFilter jsonFilterAnnotation = (JsonFilter) maybeJsonFilterAnnotation.get();
    assertEquals(Account.class.getSimpleName(), jsonFilterAnnotation.value());
  }

  @ParameterizedTest
  @MethodSource
  public void testHasEnabledLinkedDevice(final Account account, final boolean expect) {
    assertEquals(expect, account.hasEnabledLinkedDevice());
  }

  static Stream<Arguments> testHasEnabledLinkedDevice() {
    final Device enabledPrimary = mock(Device.class);
    when(enabledPrimary.isEnabled()).thenReturn(true);
    when(enabledPrimary.getId()).thenReturn(Device.PRIMARY_ID);

    final Device disabledPrimary = mock(Device.class);
    when(disabledPrimary.getId()).thenReturn(Device.PRIMARY_ID);

    final byte linked1DeviceId = Device.PRIMARY_ID + 1;
    final Device enabledLinked1 = mock(Device.class);
    when(enabledLinked1.isEnabled()).thenReturn(true);
    when(enabledLinked1.getId()).thenReturn(linked1DeviceId);

    final Device disabledLinked1 = mock(Device.class);
    when(disabledLinked1.getId()).thenReturn(linked1DeviceId);

    final byte linked2DeviceId = Device.PRIMARY_ID + 2;
    final Device enabledLinked2 = mock(Device.class);
    when(enabledLinked2.isEnabled()).thenReturn(true);
    when(enabledLinked2.getId()).thenReturn(linked2DeviceId);

    final Device disabledLinked2 = mock(Device.class);
    when(disabledLinked2.getId()).thenReturn(linked2DeviceId);

    return Stream.of(
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123", List.of(enabledPrimary)), false),
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123", List.of(enabledPrimary, disabledLinked1)),
            false),
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123",
            List.of(enabledPrimary, disabledLinked1, disabledLinked2)), false),
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123",
            List.of(enabledPrimary, enabledLinked1, disabledLinked2)), true),
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123",
            List.of(enabledPrimary, disabledLinked1, enabledLinked2)), true),
        Arguments.of(AccountsHelper.generateTestAccount("+14155550123",
            List.of(disabledLinked2, enabledLinked1, enabledLinked2)), true)
    );
  }
}
