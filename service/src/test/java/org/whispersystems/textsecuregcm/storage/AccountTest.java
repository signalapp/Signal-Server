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

import com.fasterxml.jackson.annotation.JsonFilter;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

class AccountTest {

  private final Device oldPrimaryDevice = mock(Device.class);
  private final Device recentPrimaryDevice = mock(Device.class);
  private final Device agingSecondaryDevice = mock(Device.class);
  private final Device recentSecondaryDevice = mock(Device.class);
  private final Device oldSecondaryDevice = mock(Device.class);
  private final Device deleteSyncCapableDevice = mock(Device.class);
  private final Device deleteSyncIncapableDevice = mock(Device.class);

  @BeforeEach
  void setup() {
    when(oldPrimaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    when(recentPrimaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentPrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    final byte deviceId2 = 2;
    when(agingSecondaryDevice.getId()).thenReturn(deviceId2);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.getId()).thenReturn(deviceId2);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.getId()).thenReturn(deviceId2);

    when(deleteSyncCapableDevice.getId()).thenReturn((byte) 1);
    when(deleteSyncCapableDevice.hasCapability(DeviceCapability.DELETE_SYNC)).thenReturn(true);

    when(deleteSyncIncapableDevice.getId()).thenReturn((byte) 2);
    when(deleteSyncIncapableDevice.hasCapability(DeviceCapability.DELETE_SYNC)).thenReturn(false);
  }

  @Test
  void testIsTransferSupported() {
    final Device transferCapablePrimaryDevice = mock(Device.class);
    final Device nonTransferCapablePrimaryDevice = mock(Device.class);
    final Device transferCapableLinkedDevice = mock(Device.class);

    when(transferCapablePrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(transferCapablePrimaryDevice.isPrimary()).thenReturn(true);
    when(transferCapablePrimaryDevice.hasCapability(DeviceCapability.TRANSFER)).thenReturn(true);

    when(nonTransferCapablePrimaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(nonTransferCapablePrimaryDevice.isPrimary()).thenReturn(true);
    when(nonTransferCapablePrimaryDevice.hasCapability(DeviceCapability.TRANSFER)).thenReturn(false);

    when(transferCapableLinkedDevice.getId()).thenReturn((byte) 2);
    when(transferCapableLinkedDevice.isPrimary()).thenReturn(false);
    when(transferCapableLinkedDevice.hasCapability(DeviceCapability.TRANSFER)).thenReturn(true);

    {
      final Account transferablePrimaryAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(transferCapablePrimaryDevice), "1234".getBytes());

      assertTrue(transferablePrimaryAccount.hasCapability(DeviceCapability.TRANSFER));
    }

    {
      final Account nonTransferablePrimaryAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapablePrimaryDevice), "1234".getBytes());

      assertFalse(nonTransferablePrimaryAccount.hasCapability(DeviceCapability.TRANSFER));
    }

    {
      final Account transferableLinkedAccount = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapablePrimaryDevice, transferCapableLinkedDevice), "1234".getBytes());

      assertFalse(transferableLinkedAccount.hasCapability(DeviceCapability.TRANSFER));
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
  void isDeleteSyncSupported() {
    assertTrue(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(deleteSyncCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).hasCapability(DeviceCapability.DELETE_SYNC));
    assertFalse(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(deleteSyncIncapableDevice, deleteSyncCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).hasCapability(DeviceCapability.DELETE_SYNC));
  }

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
    final TestClock clock = TestClock.pinned(Instant.ofEpochSecond(40));

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(42), false));
    account.addBadge(clock, new AccountBadge("bar", Instant.ofEpochSecond(44), true));
    account.addBadge(clock, new AccountBadge("baz", Instant.ofEpochSecond(46), true));

    assertThat(account.getBadges()).hasSize(3);

    account.removeBadge(clock, "baz");

    assertThat(account.getBadges()).hasSize(2);

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(50), false));

    assertThat(account.getBadges()).hasSize(2).element(0).satisfies(badge -> {
      assertThat(badge.id()).isEqualTo("foo");
      assertThat(badge.expiration().getEpochSecond()).isEqualTo(50);
      assertThat(badge.visible()).isFalse();
    });

    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(51), true));

    assertThat(account.getBadges()).hasSize(2).element(0).satisfies(badge -> {
      assertThat(badge.id()).isEqualTo("foo");
      assertThat(badge.expiration().getEpochSecond()).isEqualTo(51);
      assertThat(badge.visible()).isTrue();
    });

    clock.pin(Instant.ofEpochSecond(52));

    // for a merged badge, visible = true is preferred
    account.addBadge(clock, new AccountBadge("foo", Instant.ofEpochSecond(53), false));

    assertThat(account.getBadges()).hasSize(1).element(0).satisfies(badge -> {
      assertThat(badge.id()).isEqualTo("foo");
      assertThat(badge.expiration().getEpochSecond()).isEqualTo(53);
      assertThat(badge.visible()).isTrue();
    });
  }

  @Test
  public void testAccountClassJsonFilterIdMatchesClassName() {
    // Some logic relies on the @JsonFilter name being equal to the class name.
    // This test is just making sure that annotation is there and that the ID matches class name.
    final Optional<Annotation> maybeJsonFilterAnnotation = Arrays.stream(Account.class.getAnnotations())
        .filter(a -> a.annotationType().equals(JsonFilter.class))
        .findFirst();
    assertTrue(maybeJsonFilterAnnotation.isPresent());
    final JsonFilter jsonFilterAnnotation = (JsonFilter) maybeJsonFilterAnnotation.get();
    assertEquals(Account.class.getSimpleName(), jsonFilterAnnotation.value());
  }
}
