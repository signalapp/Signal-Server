/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.DevicesHelper.createDevice;
import static org.whispersystems.textsecuregcm.tests.util.DevicesHelper.setEnabled;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

class AccountTest {

  private final Device oldMasterDevice = mock(Device.class);
  private final Device recentMasterDevice = mock(Device.class);
  private final Device agingSecondaryDevice = mock(Device.class);
  private final Device recentSecondaryDevice = mock(Device.class);
  private final Device oldSecondaryDevice = mock(Device.class);

  private final Device senderKeyCapableDevice = mock(Device.class);
  private final Device senderKeyIncapableDevice = mock(Device.class);
  private final Device senderKeyIncapableExpiredDevice = mock(Device.class);

  private final Device announcementGroupCapableDevice = mock(Device.class);
  private final Device announcementGroupIncapableDevice = mock(Device.class);
  private final Device announcementGroupIncapableExpiredDevice = mock(Device.class);

  private final Device changeNumberCapableDevice = mock(Device.class);
  private final Device changeNumberIncapableDevice = mock(Device.class);
  private final Device changeNumberIncapableExpiredDevice = mock(Device.class);

  private final Device pniCapableDevice = mock(Device.class);
  private final Device pniIncapableDevice = mock(Device.class);
  private final Device pniIncapableExpiredDevice = mock(Device.class);

  private final Device storiesCapableDevice = mock(Device.class);
  private final Device storiesIncapableDevice = mock(Device.class);
  private final Device storiesIncapableExpiredDevice = mock(Device.class);

  private final Device giftBadgesCapableDevice = mock(Device.class);
  private final Device giftBadgesIncapableDevice = mock(Device.class);
  private final Device giftBadgesIncapableExpiredDevice = mock(Device.class);

  private final Device paymentActivationCapableDevice = mock(Device.class);
  private final Device paymentActivationIncapableDevice = mock(Device.class);
  private final Device paymentActivationIncapableExpiredDevice = mock(Device.class);

  @BeforeEach
  void setup() {
    when(oldMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldMasterDevice.isEnabled()).thenReturn(true);
    when(oldMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(recentMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentMasterDevice.isEnabled()).thenReturn(true);
    when(recentMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(agingSecondaryDevice.isEnabled()).thenReturn(false);
    when(agingSecondaryDevice.getId()).thenReturn(2L);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.isEnabled()).thenReturn(true);
    when(recentSecondaryDevice.getId()).thenReturn(2L);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.isEnabled()).thenReturn(false);
    when(oldSecondaryDevice.getId()).thenReturn(2L);

    when(senderKeyCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(senderKeyCapableDevice.isEnabled()).thenReturn(true);

    when(senderKeyIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, false, false, false, false, false, false, false));
    when(senderKeyIncapableDevice.isEnabled()).thenReturn(true);

    when(senderKeyIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, false, false, false, false, false, false, false));
    when(senderKeyIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(announcementGroupCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, false, false, false, false, false));
    when(announcementGroupCapableDevice.isEnabled()).thenReturn(true);

    when(announcementGroupIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(announcementGroupIncapableDevice.isEnabled()).thenReturn(true);

    when(announcementGroupIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(announcementGroupIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(changeNumberCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, true, false, false, false, false));
    when(changeNumberCapableDevice.isEnabled()).thenReturn(true);

    when(changeNumberIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(changeNumberIncapableDevice.isEnabled()).thenReturn(true);

    when(changeNumberIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(changeNumberIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(pniCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, true, false, false, false));
    when(pniCapableDevice.isEnabled()).thenReturn(true);

    when(pniIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(pniIncapableDevice.isEnabled()).thenReturn(true);

    when(pniIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(pniIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(storiesCapableDevice.getId()).thenReturn(1L);
    when(storiesCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, true, false, false));
    when(storiesCapableDevice.isEnabled()).thenReturn(true);

    when(storiesCapableDevice.getId()).thenReturn(2L);
    when(storiesIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(storiesIncapableDevice.isEnabled()).thenReturn(true);

    when(storiesCapableDevice.getId()).thenReturn(3L);
    when(storiesIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, false, false, false, false, false, false));
    when(storiesIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(giftBadgesCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, true, false));
    when(giftBadgesCapableDevice.isEnabled()).thenReturn(true);
    when(giftBadgesIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, false, false));
    when(giftBadgesIncapableDevice.isEnabled()).thenReturn(true);
    when(giftBadgesIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, false, false));
    when(giftBadgesIncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(paymentActivationCapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, true, true));
    when(paymentActivationCapableDevice.isEnabled()).thenReturn(true);
    when(paymentActivationIncapableDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, false, false));
    when(paymentActivationIncapableDevice.isEnabled()).thenReturn(true);
    when(paymentActivationIncapableExpiredDevice.getCapabilities()).thenReturn(
        new DeviceCapabilities(true, true, true, true, true, true, true, false, false));
    when(paymentActivationIncapableExpiredDevice.isEnabled()).thenReturn(false);

  }

  @Test
  void testIsEnabled() {
    final Device enabledMasterDevice = mock(Device.class);
    final Device enabledLinkedDevice = mock(Device.class);
    final Device disabledMasterDevice = mock(Device.class);
    final Device disabledLinkedDevice = mock(Device.class);

    when(enabledMasterDevice.isEnabled()).thenReturn(true);
    when(enabledLinkedDevice.isEnabled()).thenReturn(true);
    when(disabledMasterDevice.isEnabled()).thenReturn(false);
    when(disabledLinkedDevice.isEnabled()).thenReturn(false);

    when(enabledMasterDevice.getId()).thenReturn(1L);
    when(enabledLinkedDevice.getId()).thenReturn(2L);
    when(disabledMasterDevice.getId()).thenReturn(1L);
    when(disabledLinkedDevice.getId()).thenReturn(2L);

    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledMasterDevice)).isEnabled());
    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledMasterDevice, enabledLinkedDevice)).isEnabled());
    assertTrue(AccountsHelper.generateTestAccount("+14151234567", List.of(enabledMasterDevice, disabledLinkedDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledMasterDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledMasterDevice, enabledLinkedDevice)).isEnabled());
    assertFalse(AccountsHelper.generateTestAccount("+14151234567", List.of(disabledMasterDevice, disabledLinkedDevice)).isEnabled());
  }

  @Test
  void testIsTransferSupported() {
    final Device transferCapableMasterDevice = mock(Device.class);
    final Device nonTransferCapableMasterDevice = mock(Device.class);
    final Device transferCapableLinkedDevice = mock(Device.class);

    final DeviceCapabilities transferCapabilities = mock(DeviceCapabilities.class);
    final DeviceCapabilities nonTransferCapabilities = mock(DeviceCapabilities.class);

    when(transferCapableMasterDevice.getId()).thenReturn(1L);
    when(transferCapableMasterDevice.isMaster()).thenReturn(true);
    when(transferCapableMasterDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(nonTransferCapableMasterDevice.getId()).thenReturn(1L);
    when(nonTransferCapableMasterDevice.isMaster()).thenReturn(true);
    when(nonTransferCapableMasterDevice.getCapabilities()).thenReturn(nonTransferCapabilities);

    when(transferCapableLinkedDevice.getId()).thenReturn(2L);
    when(transferCapableLinkedDevice.isMaster()).thenReturn(false);
    when(transferCapableLinkedDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(transferCapabilities.isTransfer()).thenReturn(true);
    when(nonTransferCapabilities.isTransfer()).thenReturn(false);

    {
      final Account transferableMasterAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(transferCapableMasterDevice), "1234".getBytes());

      assertTrue(transferableMasterAccount.isTransferSupported());
    }

    {
      final Account nonTransferableMasterAccount =
              AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapableMasterDevice), "1234".getBytes());

      assertFalse(nonTransferableMasterAccount.isTransferSupported());
    }

    {
      final Account transferableLinkedAccount = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(nonTransferCapableMasterDevice, transferCapableLinkedDevice), "1234".getBytes());

      assertFalse(transferableLinkedAccount.isTransferSupported());
    }
  }

  @Test
  void testDiscoverableByPhoneNumber() {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(recentMasterDevice),
        "1234".getBytes());

    assertTrue(account.isDiscoverableByPhoneNumber(),
        "Freshly-loaded legacy accounts should be discoverable by phone number.");

    account.setDiscoverableByPhoneNumber(false);
    assertFalse(account.isDiscoverableByPhoneNumber());

    account.setDiscoverableByPhoneNumber(true);
    assertTrue(account.isDiscoverableByPhoneNumber());
  }

  @Test
  void isSenderKeySupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), List.of(senderKeyCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isSenderKeySupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(senderKeyCapableDevice, senderKeyIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isSenderKeySupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(senderKeyCapableDevice, senderKeyIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isSenderKeySupported()).isTrue();
  }

  @Test
  void isAnnouncementGroupSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(announcementGroupCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isAnnouncementGroupSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(announcementGroupCapableDevice, announcementGroupIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isAnnouncementGroupSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(announcementGroupCapableDevice, announcementGroupIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isAnnouncementGroupSupported()).isTrue();
  }

  @Test
  void isChangeNumberSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(changeNumberCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isChangeNumberSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(changeNumberCapableDevice, changeNumberIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isChangeNumberSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(changeNumberCapableDevice, changeNumberIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isChangeNumberSupported()).isTrue();
  }

  @Test
  void isPniSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(pniCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPniSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(pniCapableDevice, pniIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPniSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(pniCapableDevice, pniIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isPniSupported()).isTrue();
  }

  @Test
  void isStoriesSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(storiesCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isStoriesSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(storiesCapableDevice, storiesIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isStoriesSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(),
        UUID.randomUUID(), List.of(storiesCapableDevice, storiesIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isStoriesSupported()).isTrue();
  }

  @Test
  void isGiftBadgesSupported() {
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(giftBadgesCapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isGiftBadgesSupported()).isTrue();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(giftBadgesCapableDevice, giftBadgesIncapableDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isGiftBadgesSupported()).isFalse();
    assertThat(AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        List.of(giftBadgesCapableDevice, giftBadgesIncapableExpiredDevice),
        "1234".getBytes(StandardCharsets.UTF_8)).isGiftBadgesSupported()).isTrue();
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

    final List<Device> devices = List.of(createDevice(Device.MASTER_ID));

    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), devices, new byte[0]);

    assertThat(account.getNextDeviceId()).isEqualTo(2L);

    account.addDevice(createDevice(2L));

    assertThat(account.getNextDeviceId()).isEqualTo(3L);

    account.addDevice(createDevice(3L));

    setEnabled(account.getDevice(2L).orElseThrow(), false);

    assertThat(account.getNextDeviceId()).isEqualTo(4L);

    account.removeDevice(2L);

    assertThat(account.getNextDeviceId()).isEqualTo(2L);
  }

  @Test
  void replaceDevice() {
    final Device firstDevice = createDevice(Device.MASTER_ID);
    final Device secondDevice = createDevice(Device.MASTER_ID);
    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), List.of(firstDevice), new byte[0]);

    assertEquals(List.of(firstDevice), account.getDevices());

    account.addDevice(secondDevice);

    assertEquals(List.of(secondDevice), account.getDevices());
  }

  @Test
  void addAndRemoveBadges() {
    final Account account = AccountsHelper.generateTestAccount("+14151234567", UUID.randomUUID(), UUID.randomUUID(), List.of(createDevice(Device.MASTER_ID)), new byte[0]);
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
}
