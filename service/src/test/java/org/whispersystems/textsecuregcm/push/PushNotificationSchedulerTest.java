/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestClock;

class PushNotificationSchedulerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private Account account;
  private Device device;

  private APNSender apnSender;
  private FcmSender fcmSender;
  private TestClock clock;

  private PushNotificationScheduler pushNotificationScheduler;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final String ACCOUNT_NUMBER = "+18005551234";
  private static final byte DEVICE_ID = 1;
  private static final String APN_ID = RandomStringUtils.secure().nextAlphanumeric(32);
  private static final String GCM_ID = RandomStringUtils.secure().nextAlphanumeric(32);

  @BeforeEach
  void setUp() throws Exception {

    device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);
    when(device.getApnId()).thenReturn(APN_ID);
    when(device.getGcmId()).thenReturn(GCM_ID);
    when(device.getLastSeen()).thenReturn(System.currentTimeMillis());

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_UUID);
    when(account.getNumber()).thenReturn(ACCOUNT_NUMBER);
    when(account.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.getByE164(ACCOUNT_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByAccountIdentifierAsync(ACCOUNT_UUID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    apnSender = mock(APNSender.class);
    fcmSender = mock(FcmSender.class);
    clock = TestClock.now();

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    when(fcmSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    pushNotificationScheduler = new PushNotificationScheduler(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        apnSender, fcmSender, accountsManager, clock, 1, 1, mock(ScheduledExecutorService.class));
  }

  @ParameterizedTest
  @EnumSource(PushNotification.TokenType.class)
  void testScheduleBackgroundNotificationWithNoRecentApnsNotification(PushNotification.TokenType tokenType) throws ExecutionException, InterruptedException {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    clock.pin(now);

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getLastBackgroundApnsNotificationTimestamp(account, device));

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(tokenType, account, device));

    pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device).toCompletableFuture().get();

    assertEquals(Optional.of(now),
        pushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(tokenType, account, device));
  }

  @ParameterizedTest
  @EnumSource(PushNotification.TokenType.class)
  void testScheduleBackgroundNotificationWithRecentNotification(PushNotification.TokenType tokenType) throws ExecutionException, InterruptedException {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final Instant recentNotificationTimestamp =
        now.minus(PushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD.dividedBy(2));

    // Insert a timestamp for a recently-sent background push notification
    clock.pin(Instant.ofEpochMilli(recentNotificationTimestamp.toEpochMilli()));
    pushNotificationScheduler.sendBackgroundNotification(tokenType, account, device);

    clock.pin(now);
    pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device).toCompletableFuture().get();

    final Instant expectedScheduledTimestamp =
        recentNotificationTimestamp.plus(PushNotificationScheduler.BACKGROUND_NOTIFICATION_PERIOD);

    assertEquals(Optional.of(expectedScheduledTimestamp),
        pushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(tokenType, account, device));
  }

  @ParameterizedTest
  @EnumSource(PushNotification.TokenType.class)
  void testCancelBackgroundApnsNotifications(PushNotification.TokenType tokenType) {
    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    clock.pin(now);

    pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device).toCompletableFuture().join();
    pushNotificationScheduler.cancelBackgroundNotifications(tokenType, account, device).join();

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getLastBackgroundApnsNotificationTimestamp(account, device));

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(tokenType, account, device));
  }

  @ParameterizedTest
  @EnumSource(PushNotification.TokenType.class)
  void testProcessScheduledBackgroundNotifications(PushNotification.TokenType tokenType) {
    final PushNotificationScheduler.NotificationWorker worker = pushNotificationScheduler.new NotificationWorker(1);

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    clock.pin(Instant.ofEpochMilli(now.toEpochMilli()));
    pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device).toCompletableFuture().join();

    final int slot =
        SlotHash.getSlot(PushNotificationScheduler.getPendingBackgroundNotificationQueueKey(tokenType, account, device));

    clock.pin(Instant.ofEpochMilli(now.minusMillis(1).toEpochMilli()));
    assertEquals(0, worker.processScheduledBackgroundNotifications(tokenType, slot));

    clock.pin(now);
    assertEquals(1, worker.processScheduledBackgroundNotifications(tokenType, slot));

    final ArgumentCaptor<PushNotification> notificationCaptor = ArgumentCaptor.forClass(PushNotification.class);
    verify(switch (tokenType) {
      case FCM -> fcmSender;
      case APN -> apnSender;
    }).sendNotification(notificationCaptor.capture());

    final PushNotification pushNotification = notificationCaptor.getValue();

    assertEquals(tokenType, pushNotification.tokenType());
    assertEquals(switch (tokenType) {
      case FCM -> GCM_ID;
      case APN -> APN_ID;
    }, pushNotification.deviceToken());
    assertEquals(account, pushNotification.destination());
    assertEquals(device, pushNotification.destinationDevice());
    assertEquals(PushNotification.NotificationType.NOTIFICATION, pushNotification.notificationType());
    assertFalse(pushNotification.urgent());

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledBackgroundNotificationTimestamp(tokenType, account, device));
  }

  @ParameterizedTest
  @EnumSource(PushNotification.TokenType.class)
  void testProcessScheduledBackgroundNotificationsCancelled(PushNotification.TokenType tokenType) throws ExecutionException, InterruptedException {
    final PushNotificationScheduler.NotificationWorker worker = pushNotificationScheduler.new NotificationWorker(1);

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    clock.pin(now);
    pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device).toCompletableFuture().get();
    pushNotificationScheduler.cancelScheduledNotifications(account, device).toCompletableFuture().get();

    final int slot =
        SlotHash.getSlot(PushNotificationScheduler.getPendingBackgroundNotificationQueueKey(tokenType, account, device));

    assertEquals(0, worker.processScheduledBackgroundNotifications(tokenType, slot));

    verify(apnSender, never()).sendNotification(any());
  }

  @Test
  void testScheduleDelayedNotification() {
    clock.pin(Instant.now());

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledDelayedNotificationTimestamp(account, device));

    pushNotificationScheduler.scheduleDelayedNotification(account, device, Duration.ofMinutes(1)).join();

    assertEquals(Optional.of(clock.instant().truncatedTo(ChronoUnit.MILLIS).plus(Duration.ofMinutes(1))),
        pushNotificationScheduler.getNextScheduledDelayedNotificationTimestamp(account, device));

    pushNotificationScheduler.scheduleDelayedNotification(account, device, Duration.ofMinutes(2)).join();

    assertEquals(Optional.of(clock.instant().truncatedTo(ChronoUnit.MILLIS).plus(Duration.ofMinutes(2))),
        pushNotificationScheduler.getNextScheduledDelayedNotificationTimestamp(account, device));
  }

  @Test
  void testCancelDelayedNotification() {
    pushNotificationScheduler.scheduleDelayedNotification(account, device, Duration.ofMinutes(1)).join();
    pushNotificationScheduler.cancelDelayedNotifications(account, device).join();

    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledDelayedNotificationTimestamp(account, device));
  }

  @Test
  void testProcessScheduledDelayedNotifications() {
    final PushNotificationScheduler.NotificationWorker worker = pushNotificationScheduler.new NotificationWorker(1);
    final int slot = SlotHash.getSlot(PushNotificationScheduler.getDelayedNotificationQueueKey(account, device));

    clock.pin(Instant.now());

    pushNotificationScheduler.scheduleDelayedNotification(account, device, Duration.ofMinutes(1)).join();

    assertEquals(0, worker.processScheduledDelayedNotifications(slot));

    clock.pin(clock.instant().plus(Duration.ofMinutes(1)));

    assertEquals(1, worker.processScheduledDelayedNotifications(slot));
    assertEquals(Optional.empty(),
        pushNotificationScheduler.getNextScheduledDelayedNotificationTimestamp(account, device));
  }

  @ParameterizedTest
  @CsvSource({
      "1, true",
      "0, false",
  })
  void testDedicatedProcessDynamicConfiguration(final int dedicatedThreadCount, final boolean expectActivity)
      throws Exception {

    final FaultTolerantRedisClusterClient redisCluster = mock(FaultTolerantRedisClusterClient.class);
    when(redisCluster.withCluster(any())).thenReturn(0L);

    final AccountsManager accountsManager = mock(AccountsManager.class);

    pushNotificationScheduler = new PushNotificationScheduler(redisCluster, apnSender, fcmSender,
        accountsManager, dedicatedThreadCount, 1, mock(ScheduledExecutorService.class));

    pushNotificationScheduler.start();
    pushNotificationScheduler.stop();

    if (expectActivity) {
      verify(redisCluster, atLeastOnce()).withCluster(any());
    } else {
      verifyNoInteractions(redisCluster);
      verifyNoInteractions(accountsManager);
      verifyNoInteractions(apnSender);
    }
  }
}
