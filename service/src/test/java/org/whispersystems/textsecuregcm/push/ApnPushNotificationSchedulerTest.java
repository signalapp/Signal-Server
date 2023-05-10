/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicScheduledApnNotificationSendingConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;

class ApnPushNotificationSchedulerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private Account account;
  private Device device;

  private APNSender apnSender;
  private TestClock clock;

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
    clock = TestClock.now();

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
        DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicScheduledApnNotificationSendingConfiguration scheduledApnNotificationSendingConfiguration = new DynamicScheduledApnNotificationSendingConfiguration(
        true, true);
    when(dynamicConfiguration.getScheduledApnNotificationSendingConfiguration()).thenReturn(
        scheduledApnNotificationSendingConfiguration);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    apnPushNotificationScheduler = new ApnPushNotificationScheduler(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        apnSender, accountsManager, clock, Optional.empty(), dynamicConfigurationManager);
  }

  @Test
  void testClusterInsert() throws ExecutionException, InterruptedException {
    final String endpoint = ApnPushNotificationScheduler.getEndpointKey(account, device);
    final long currentTimeMillis = System.currentTimeMillis();

    assertTrue(
        apnPushNotificationScheduler.getPendingDestinationsForRecurringVoipNotifications(SlotHash.getSlot(endpoint), 1).isEmpty());

    clock.pin(Instant.ofEpochMilli(currentTimeMillis - 30_000));
    apnPushNotificationScheduler.scheduleRecurringVoipNotification(account, device).toCompletableFuture().get();

    clock.pin(Instant.ofEpochMilli(currentTimeMillis));
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
  void testProcessRecurringVoipNotifications() throws ExecutionException, InterruptedException {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();
    final long currentTimeMillis = System.currentTimeMillis();

    clock.pin(Instant.ofEpochMilli(currentTimeMillis - 30_000));
    apnPushNotificationScheduler.scheduleRecurringVoipNotification(account, device).toCompletableFuture().get();

    clock.pin(Instant.ofEpochMilli(currentTimeMillis));

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
  void testScheduleBackgroundNotificationWithNoRecentNotification() throws ExecutionException, InterruptedException {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    clock.pin(now);

    assertEquals(Optional.empty(),
        apnPushNotificationScheduler.getLastBackgroundNotificationTimestamp(account, device));

    assertEquals(Optional.empty(),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));

    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device).toCompletableFuture().get();

    assertEquals(Optional.of(now),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));
  }

  @Test
  void testScheduleBackgroundNotificationWithRecentNotification() throws ExecutionException, InterruptedException {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final Instant recentNotificationTimestamp =
        now.minus(ApnPushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD.dividedBy(2));

    // Insert a timestamp for a recently-sent background push notification
    clock.pin(Instant.ofEpochMilli(recentNotificationTimestamp.toEpochMilli()));
    apnPushNotificationScheduler.sendBackgroundNotification(account, device);

    clock.pin(now);
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device).toCompletableFuture().get();

    final Instant expectedScheduledTimestamp =
        recentNotificationTimestamp.plus(ApnPushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD);

    assertEquals(Optional.of(expectedScheduledTimestamp),
        apnPushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(account, device));
  }

  @Test
  void testProcessScheduledBackgroundNotifications() throws ExecutionException, InterruptedException {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    clock.pin(Instant.ofEpochMilli(now.toEpochMilli()));
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device).toCompletableFuture().get();

    final int slot =
        SlotHash.getSlot(ApnPushNotificationScheduler.getPendingBackgroundNotificationQueueKey(account, device));

    clock.pin(Instant.ofEpochMilli(now.minusMillis(1).toEpochMilli()));
    assertEquals(0, worker.processScheduledBackgroundNotifications(slot));

    clock.pin(now);
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
  void testProcessScheduledBackgroundNotificationsCancelled() throws ExecutionException, InterruptedException {
    final ApnPushNotificationScheduler.NotificationWorker worker = apnPushNotificationScheduler.new NotificationWorker();

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    clock.pin(now);
    apnPushNotificationScheduler.scheduleBackgroundNotification(account, device).toCompletableFuture().get();
    apnPushNotificationScheduler.cancelScheduledNotifications(account, device).toCompletableFuture().get();

    final int slot =
        SlotHash.getSlot(ApnPushNotificationScheduler.getPendingBackgroundNotificationQueueKey(account, device));

    assertEquals(0, worker.processScheduledBackgroundNotifications(slot));

    verify(apnSender, never()).sendNotification(any());
  }

  @ParameterizedTest
  @CsvSource({
      "true, false, true, true",
      "true, true, false, false",
      "false, true, false, true",
      "false, false, true, false"
  })
  void testDedicatedProcessDynamicConfiguration(final boolean dedicatedProcess, final boolean enabledForServer,
      final boolean enabledForDedicatedProcess, final boolean expectActivity) throws Exception {

    final FaultTolerantRedisCluster redisCluster = mock(FaultTolerantRedisCluster.class);
    when(redisCluster.withCluster(any())).thenReturn(0L);

    final AccountsManager accountsManager = mock(AccountsManager.class);

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
        DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    final DynamicScheduledApnNotificationSendingConfiguration scheduledApnNotificationSendingConfiguration = new DynamicScheduledApnNotificationSendingConfiguration(
        enabledForServer, enabledForDedicatedProcess);
    when(dynamicConfiguration.getScheduledApnNotificationSendingConfiguration()).thenReturn(
        scheduledApnNotificationSendingConfiguration);

    apnPushNotificationScheduler = new ApnPushNotificationScheduler(redisCluster, apnSender,
        accountsManager, dedicatedProcess ? Optional.of(4) : Optional.empty(), dynamicConfigurationManager);

    apnPushNotificationScheduler.start();
    apnPushNotificationScheduler.stop();

    if (expectActivity) {
      verify(redisCluster, atLeastOnce()).withCluster(any());
    } else {
      verifyNoInteractions(redisCluster);
      verifyNoInteractions(accountsManager);
      verifyNoInteractions(apnSender);
    }
  }
}
