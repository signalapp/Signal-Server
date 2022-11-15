/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPushNotificationConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.Util;

class PushNotificationManagerTest {

  private AccountsManager accountsManager;
  private APNSender apnSender;
  private FcmSender fcmSender;
  private ApnPushNotificationScheduler apnPushNotificationScheduler;
  private PushLatencyManager pushLatencyManager;
  private DynamicPushNotificationConfiguration pushNotificationConfiguration;

  private PushNotificationManager pushNotificationManager;

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    apnSender = mock(APNSender.class);
    fcmSender = mock(FcmSender.class);
    apnPushNotificationScheduler = mock(ApnPushNotificationScheduler.class);
    pushLatencyManager = mock(PushLatencyManager.class);
    pushNotificationConfiguration = mock(DynamicPushNotificationConfiguration.class);

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPushNotificationConfiguration()).thenReturn(pushNotificationConfiguration);
    when(pushNotificationConfiguration.isLowUrgencyEnabled()).thenReturn(true);

    AccountsHelper.setupMockUpdate(accountsManager);

    pushNotificationManager = new PushNotificationManager(accountsManager, apnSender, fcmSender,
        apnPushNotificationScheduler, pushLatencyManager, dynamicConfigurationManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void sendNewMessageNotification(final boolean urgent) throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final String deviceToken = "token";

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getGcmId()).thenReturn(deviceToken);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    when(fcmSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNewMessageNotification(account, Device.MASTER_ID, urgent);
    verify(fcmSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device, urgent));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void sendNewMessageNotificationLowUrgencyDisabled(final boolean urgent) throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final String deviceToken = "token";

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getApnId()).thenReturn(deviceToken);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    when(pushNotificationConfiguration.isLowUrgencyEnabled()).thenReturn(false);

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNewMessageNotification(account, Device.MASTER_ID, urgent);

    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.APN, PushNotification.NotificationType.NOTIFICATION, null, account, device, true));
  }

  @Test
  void sendRegistrationChallengeNotification() {
    final String deviceToken = "token";
    final String challengeToken = "challenge";

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendRegistrationChallengeNotification(deviceToken, PushNotification.TokenType.APN_VOIP, challengeToken);
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null, true));
  }

  @Test
  void sendRateLimitChallengeNotification() throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final String deviceToken = "token";
    final String challengeToken = "challenge";

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getApnId()).thenReturn(deviceToken);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendRateLimitChallengeNotification(account, challengeToken);
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.APN, PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, account, device, true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendNotificationFcm(final boolean urgent) {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device, urgent);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(fcmSender).sendNotification(pushNotification);
    verifyNoInteractions(apnSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verifyNoInteractions(apnPushNotificationScheduler);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendNotificationApn(final boolean urgent) {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.APN, PushNotification.NotificationType.NOTIFICATION, null, account, device, urgent);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);

    if (urgent) {
      verify(apnSender).sendNotification(pushNotification);
      verifyNoInteractions(apnPushNotificationScheduler);
    } else {
      verifyNoInteractions(apnSender);
      verify(apnPushNotificationScheduler).scheduleBackgroundNotification(account, device);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendNotificationApnVoip(final boolean urgent) {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device, urgent);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(apnSender).sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verify(apnPushNotificationScheduler).scheduleRecurringVoipNotification(account, device);
    verify(apnPushNotificationScheduler, never()).scheduleBackgroundNotification(any(), any());
  }

  @Test
  void testSendNotificationUnregisteredFcm() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getGcmId()).thenReturn("token");
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device, true);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, null, true)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(accountsManager).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verifyNoInteractions(apnSender);
    verifyNoInteractions(apnPushNotificationScheduler);
  }

  @Test
  void testSendNotificationUnregisteredApn() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device, true);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, null, true)));

    pushNotificationManager.sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verify(apnPushNotificationScheduler).cancelScheduledNotifications(account, device);
  }

  @Test
  void testHandleMessagesRetrieved() {
    final UUID accountIdentifier = UUID.randomUUID();
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final String userAgent = HttpHeaders.USER_AGENT;

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(device.getId()).thenReturn(Device.MASTER_ID);

    pushNotificationManager.handleMessagesRetrieved(account, device, userAgent);

    verify(pushLatencyManager).recordQueueRead(accountIdentifier, Device.MASTER_ID, userAgent);
    verify(apnPushNotificationScheduler).cancelScheduledNotifications(account, device);
  }
}
