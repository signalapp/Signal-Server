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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.Util;

class PushNotificationManagerTest {

  private AccountsManager accountsManager;
  private APNSender apnSender;
  private FcmSender fcmSender;
  private ApnFallbackManager apnFallbackManager;

  private PushNotificationManager pushNotificationManager;

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    apnSender = mock(APNSender.class);
    fcmSender = mock(FcmSender.class);
    apnFallbackManager = mock(ApnFallbackManager.class);

    AccountsHelper.setupMockUpdate(accountsManager);

    pushNotificationManager = new PushNotificationManager(accountsManager, apnSender, fcmSender, apnFallbackManager);
  }

  @Test
  void sendNewMessageNotification() throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final String deviceToken = "token";

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getGcmId()).thenReturn(deviceToken);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    when(fcmSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNewMessageNotification(account, Device.MASTER_ID);
    verify(fcmSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device));
  }

  @Test
  void sendRegistrationChallengeNotification() {
    final String deviceToken = "token";
    final String challengeToken = "challenge";

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendRegistrationChallengeNotification(deviceToken, PushNotification.TokenType.APN_VOIP, challengeToken);
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null));
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
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.TokenType.APN, PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, account, device));
  }

  @Test
  void testSendNotification() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(fcmSender).sendNotification(pushNotification);
    verifyNoInteractions(apnSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verifyNoInteractions(apnFallbackManager);
  }

  @Test
  void testSendNotificationApnVoip() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, null, false)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(apnSender).sendNotification(pushNotification);
    verifyNoInteractions(fcmSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verify(apnFallbackManager).schedule(account, device);
  }

  @Test
  void testSendNotificationUnregisteredFcm() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(device.getGcmId()).thenReturn("token");
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, account, device);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, null, true)));

    pushNotificationManager.sendNotification(pushNotification);

    verify(accountsManager).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verifyNoInteractions(apnSender);
    verifyNoInteractions(apnFallbackManager);
  }

  @Test
  void testSendNotificationUnregisteredApn() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.MASTER_ID);
    when(account.getDevice(Device.MASTER_ID)).thenReturn(Optional.of(device));

    final PushNotification pushNotification = new PushNotification(
        "token", PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, null, true)));

    pushNotificationManager.sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);
    verify(accountsManager, never()).updateDevice(eq(account), eq(Device.MASTER_ID), any());
    verify(device, never()).setUninstalledFeedbackTimestamp(Util.todayInMillis());
    verify(apnFallbackManager).cancel(account, device);
  }
}
