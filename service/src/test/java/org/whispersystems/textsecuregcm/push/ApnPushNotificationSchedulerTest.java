/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

class ApnPushNotificationSchedulerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private Account account;
  private Device device;

  private APNSender apnSender;
  private Clock clock;

  private ApnPushNotificationScheduler apnPushNotificationScheduler;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final String ACCOUNT_NUMBER = "+18005551234";
  private static final long DEVICE_ID = 1L;
  private static final String APN_ID = RandomStringUtils.randomAlphanumeric(32);
  private static final String VOIP_APN_ID = RandomStringUtils.randomAlphanumeric(32);

  @BeforeEach
  void setUp() throws Exception {

    device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);
    when(device.getApnId()).thenReturn(APN_ID);
    when(device.getVoipApnId()).thenReturn(VOIP_APN_ID);
    when(device.getLastSeen()).thenReturn(System.currentTimeMillis());

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    when(account.getNumber()).thenReturn(ACCOUNT_NUMBER);
    when(account.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.getByE164(ACCOUNT_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByAccountIdentifier(ACCOUNT_UUID)).thenReturn(Optional.of(account));

    apnSender = mock(APNSender.class);
    clock = mock(Clock.class);

    apnPushNotificationScheduler = new ApnPushNotificationScheduler(REDIS_CLUSTER_EXTENSION.getRedisCluster(), apnSender, accountsManager, clock);
  }

  @Test
  void testClusterInsert() {
    final String endpoint = ApnPushNotificationScheduler.getEndpointKey(account, device);
    final long currentTimeMillis = System.currentTimeMillis();

    assertTrue(
        apnPushNotificationScheduler.getPendingDestinationsForRecurringVoipNotifications(SlotHash.getSlot(endpoint), 1).isEmpty());

    when(clock.millis()).thenReturn(currentTimeMillis - 30_000);
    apnPushNotificationScheduler.scheduleRecurringVoipNotification(account, device);

    when(clock.millis()).thenReturn(currentTimeMillis);
    final List<String> pendingDestinations = apnPushNotificationScheduler.getPendingDestinationsForRecurringVoipNotifications(SlotHash.getSlot(endpoint), 2);
    assertEquals(1, pendingDestinations.size());

    final Optional<Pair<String, Long>> maybeUuidAndDeviceId = ApnPushNotificationScheduler.getSeparated(
        pendingDestinations.get(0));

    assertTrue(maybeUuidAndDeviceId.isPresent());
    assertEquals(ACCOUNT_UUID.toString(), maybeUuidAndDeviceId.get().first());
    assertEquals(DEVICE_ID, (long) maybeUuidAndDeviceId.get().second());

    assertTrue(
        apnPushNotificationScheduler.getPendingDestinationsForRecurringVoipNotifications(SlotHash.getSlot(endpoint), 1).isEmpty());
  }

  @Test
  void testProcessRecurringVoipNotifications() {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();
    final long currentTimeMillis = System.currentTimeMillis();

    when(clock.millis()).thenReturn(currentTimeMillis - 30_000);
    apnPushNotificationScheduler.scheduleRecurringVoipNotification(account, device);

    when(clock.millis()).thenReturn(currentTimeMillis);

    final int slot = SlotHash.getSlot(ApnPushNotificationScheduler.getEndpointKey(account, device));

    assertEquals(1, worker.processRecurringVoipNotifications(slot));

    final ArgumentCaptor<PushNotification> notificationCaptor = ArgumentCaptor.forClass(PushNotification.class);
    verify(apnSender).sendNotification(notificationCaptor.capture());

    final PushNotification pushNotification = notificationCaptor.getValue();

    assertEquals(VOIP_APN_ID, pushNotification.deviceToken());
    assertEquals(account, pushNotification.destination());
    assertEquals(device, pushNotification.destinationDevice());

    assertEquals(0, worker.processRecurringVoipNotifications(slot));
  }

  @Test
  void testScheduleBackgroundNotificationWithNoRecentNotification() {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    when(clock.millis()).thenReturn(now.toEpochMilli());

    assertEquals(Optional.empty(),
        apnPushNotificationScheduler.getLastBackgroundNotificationTimestamp(account, device));

    assertEquals(Optional.empty(),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));

    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device);

    assertEquals(Optional.of(now),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));
  }

  @Test
  void testScheduleBackgroundNotificationWithRecentNotification() {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final Instant recentNotificationTimestamp =
        now.minus(ApnPushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD.dividedBy(2));

    // Insert a timestamp for a recently-sent background push notification
    when(clock.millis()).thenReturn(recentNotificationTimestamp.toEpochMilli());
    apnPushNotificationScheduler.sendBackgroundNotification(account, device);

    when(clock.millis()).thenReturn(now.toEpochMilli());
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device);

    final Instant expectedScheduledTimestamp =
        recentNotificationTimestamp.plus(ApnPushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD);

    assertEquals(Optional.of(expectedScheduledTimestamp),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));
  }

  @Test
  void testProcessScheduledBackgroundNotifications() {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    when(clock.millis()).thenReturn(now.toEpochMilli());
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device);

    final int slot =
        SlotHash.getSlot(ApnPushNotificationScheduler.getPendingBackgroundNotificationQueueKey(account, device));

    when(clock.millis()).thenReturn(now.minusMillis(1).toEpochMilli());
    assertEquals(0, worker.processScheduledBackgroundNotifications(slot));

    when(clock.millis()).thenReturn(now.toEpochMilli());
    assertEquals(1, worker.processScheduledBackgroundNotifications(slot));

    final ArgumentCaptor<PushNotification> notificationCaptor = ArgumentCaptor.forClass(PushNotification.class);
    verify(apnSender).sendNotification(notificationCaptor.capture());

    final PushNotification pushNotification = notificationCaptor.getValue();

    assertEquals(PushNotification.TokenType.APN, pushNotification.tokenType());
    assertEquals(APN_ID, pushNotification.deviceToken());
    assertEquals(account, pushNotification.destination());
    assertEquals(device, pushNotification.destinationDevice());
    assertEquals(PushNotification.NotificationType.NOTIFICATION, pushNotification.notificationType());
    assertFalse(pushNotification.urgent());

    assertEquals(0, worker.processRecurringVoipNotifications(slot));
  }

  @Test
  void testProcessScheduledBackgroundNotificationsCancelled() {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    when(clock.millis()).thenReturn(now.toEpochMilli());
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device);
    apnPushNotificationScheduler.cancelScheduledNotifications(account, device);

    final int slot =
        SlotHash.getSlot(ApnPushNotificationScheduler.getPendingBackgroundNotificationQueueKey(account, device));

    assertEquals(0, worker.processScheduledBackgroundNotifications(slot));

    verify(apnSender, never()).sendNotification(any());
  }
}
