/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.net.HttpHeaders;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.PushNotification.PushToken;
import org.whispersystems.textsecuregcm.push.PushNotification.TokenType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class PushNotificationManagerTest {

  private AccountsManager accountsManager;
  private APNSender apnSender;
  private FcmSender fcmSender;
  private WebPushSender webPushSender;
  private PushNotificationScheduler pushNotificationScheduler;

  private PushNotificationManager pushNotificationManager;

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    apnSender = mock(APNSender.class);
    fcmSender = mock(FcmSender.class);
    webPushSender = mock(WebPushSender.class);
    pushNotificationScheduler = mock(PushNotificationScheduler.class);

    AccountsHelper.setupMockUpdate(accountsManager);

    pushNotificationManager = new PushNotificationManager(accountsManager, apnSender, fcmSender, webPushSender,
        pushNotificationScheduler);
  }

  @Test
  void sendNewUrgentMessageNotification() throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final PushToken<?> deviceToken = new PushToken.FCM("token");

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getGcmId()).thenReturn((String) deviceToken.value());
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));

      when(fcmSender.sendNotification(any()))
          .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));
    pushNotificationManager.sendNewMessageNotification(account, Device.PRIMARY_ID, true);
    verify(fcmSender).sendNotification(new PushNotification(deviceToken, PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null));
  }

  @Test
  void sendNewNonUrgentMessageNotification() throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final PushToken<?> deviceToken = new PushToken.FCM("token");

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getGcmId()).thenReturn((String) deviceToken.value());
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));

    when(pushNotificationScheduler.scheduleBackgroundNotification(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    pushNotificationManager.sendNewMessageNotification(account, Device.PRIMARY_ID, false);
    verify(pushNotificationScheduler).scheduleBackgroundNotification(PushNotification.TokenType.FCM, account, device);
  }


  @Test
  void sendRegistrationChallengeNotification() {
    final PushToken<?> deviceToken = new PushToken.APN("token");
    final String challengeToken = "challenge";

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    pushNotificationManager.sendRegistrationChallengeNotification(deviceToken, PushNotification.TokenType.APN, challengeToken);
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null, true, null));
  }

  @Test
  void sendRateLimitChallengeNotification() throws NotPushRegisteredException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final PushToken<?> deviceToken = new PushToken.APN("token");
    final String challengeToken = "challenge";

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getApnId()).thenReturn((String) deviceToken.value());
    when(account.getPrimaryDevice()).thenReturn(device);

    when(apnSender.sendNotification(any()))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    pushNotificationManager.sendRateLimitChallengeNotification(account, challengeToken);
    verify(apnSender).sendNotification(new PushNotification(deviceToken, PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, account, device, true, null));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void sendAttemptLoginNotification(final int tokenTypeOrd) throws NotPushRegisteredException, JsonProcessingException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final TokenType tokenType = TokenType.values()[tokenTypeOrd];
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);

    final PushToken<?> deviceToken = switch(tokenType) {
      case TokenType.APN -> new PushToken.APN("token");
      case TokenType.FCM -> new PushToken.FCM("token");
      case TokenType.WEBPUSH -> new PushToken.WEBPUSH(webPushSub);
    };

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    switch (tokenType) {
      case TokenType.APN -> {
        when(device.getApnId()).thenReturn((String) deviceToken.value());
        when(apnSender.sendNotification(any()))
            .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));
      }
      case TokenType.FCM -> {
        when(device.getGcmId()).thenReturn((String) deviceToken.value());
        when(fcmSender.sendNotification(any()))
            .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));
      }
      case TokenType.WEBPUSH -> {
        when(device.getWebPush()).thenReturn((WebPushSubscription) deviceToken.value());
        when(webPushSender.sendNotification(any()))
            .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));
      }
    }

    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));

    pushNotificationManager.sendAttemptLoginNotification(account, "someContext");

    switch (tokenType) {
      case TokenType.APN -> {
        verify(apnSender).sendNotification(new PushNotification(deviceToken,
            PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY, "someContext", account, device, true, null));
      }
      case TokenType.FCM -> {
        verify(fcmSender, times(1)).sendNotification(new PushNotification(deviceToken,
            PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY, "someContext", account, device, true, null));
      }
      case TokenType.WEBPUSH -> {
        verify(webPushSender, times(1)).sendNotification(new PushNotification(deviceToken,
            PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY, "someContext", account, device, true, null));
      }
    }
  }

  @Test
  void testSendNotificationFcm() {
    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getAccountIdentifier()).thenReturn(accountIdentifier);
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));

    AccountsHelper.setupMockGet(accountsManager, account);

    final PushNotification pushNotification = new PushNotification(
        new PushToken.FCM("token"), PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    pushNotificationManager.sendNotification(pushNotification);

    verify(fcmSender).sendNotification(pushNotification);
    verifyNoInteractions(apnSender);
    verifyNoInteractions(webPushSender);
    verify(accountsManager, never()).updateDevice(eq(accountIdentifier), eq(Device.PRIMARY_ID), any());
    verify(device, never()).setGcmId(any());
    verifyNoInteractions(pushNotificationScheduler);
  }

  @CartesianTest
  void testSendOrScheduleNotification(
      @CartesianTest.Enum(PushNotification.TokenType.class) PushNotification.TokenType tokenType,
      @CartesianTest.Values(booleans = {false, true}) final boolean urgent
    ) throws JsonProcessingException {

    final boolean expectSchedule = !urgent;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final UUID aci = UUID.randomUUID();

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
    when(account.getAccountIdentifier()).thenReturn(aci);

    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);
    final PushToken<?> deviceToken = switch(tokenType) {
      case TokenType.APN -> new PushToken.APN("token");
      case TokenType.FCM -> new PushToken.FCM("token");
      case TokenType.WEBPUSH -> new PushToken.WEBPUSH(webPushSub);
    };

    final PushNotification pushNotification = new PushNotification(
       deviceToken, PushNotification.NotificationType.NOTIFICATION, null, account, device, urgent, null);

    final PushNotificationSender sender = switch (tokenType) {
      case FCM -> fcmSender;
      case APN -> apnSender;
      case WEBPUSH -> webPushSender;
    };
    when(sender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty())));

    if (expectSchedule) {
      when(pushNotificationScheduler.scheduleBackgroundNotification(tokenType, account, device))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    pushNotificationManager.sendNotification(pushNotification);

    if (!expectSchedule) {
      verify(sender).sendNotification(pushNotification);
      verifyNoInteractions(pushNotificationScheduler);
    } else {
      verifyNoInteractions(sender);
      verify(pushNotificationScheduler).scheduleBackgroundNotification(tokenType, account, device);
    }
  }

  @Test
  void testSendNotificationUnregisteredFcm() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final UUID aci = UUID.randomUUID();
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getGcmId()).thenReturn("token");
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
    when(account.getAccountIdentifier()).thenReturn(aci);
    when(account.getAccountIdentifier()).thenReturn(aci);
    when(accountsManager.getByAccountIdentifier(aci)).thenReturn(Optional.of(account));

    final PushNotification pushNotification = new PushNotification(
        new PushToken.FCM("token"), PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null);

    when(fcmSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.empty(), true, Optional.empty())));

    pushNotificationManager.sendNotification(pushNotification);

    verify(accountsManager).updateDevice(eq(aci), eq(Device.PRIMARY_ID), any());
    verify(device).setGcmId(null);
    verifyNoInteractions(apnSender);
    verifyNoInteractions(webPushSender);
    verifyNoInteractions(pushNotificationScheduler);
  }

  @Test
  void testSendNotificationUnregisteredApn() {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final UUID aci = UUID.randomUUID();
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getApnId()).thenReturn("apns-token");
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
    when(account.getAccountIdentifier()).thenReturn(aci);
    when(account.getAccountIdentifier()).thenReturn(aci);
    when(accountsManager.getByAccountIdentifier(aci)).thenReturn(Optional.of(account));

    final PushNotification pushNotification = new PushNotification(
        new PushToken.APN("token"), PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.empty(), true, Optional.empty())));

    when(pushNotificationScheduler.cancelScheduledNotifications(account, device))
        .thenReturn(CompletableFuture.completedFuture(null));

    pushNotificationManager.sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);
    verifyNoInteractions(webPushSender);
    verify(accountsManager).updateDevice(eq(aci), eq(Device.PRIMARY_ID), any());
    verify(device).setApnId(null);
    verify(pushNotificationScheduler).cancelScheduledNotifications(account, device);
  }

  @Test
  void testSendNotificationUnregisteredWebPush() throws JsonProcessingException {
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final UUID aci = UUID.randomUUID();
    final WebPushSubscription webPushSub = SystemMapper.jsonMapper().readValue("""
        {
          "endpoint": "https://domain.tld/random1",
          "auth": "BTBZMqHH6r4Tts7J_aSIgg",
          "publicKey": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
        }
      """, WebPushSubscription.class);

    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getWebPush()).thenReturn(webPushSub);
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
    when(account.getUuid()).thenReturn(aci);
    when(accountsManager.getByAccountIdentifier(aci)).thenReturn(Optional.of(account));

    final PushNotification pushNotification = new PushNotification(
        new PushToken.WEBPUSH(webPushSub), PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null);

    when(webPushSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.empty(), true, Optional.empty())));

    pushNotificationManager.sendNotification(pushNotification);

    verify(accountsManager).updateDevice(eq(account), eq(Device.PRIMARY_ID), any());
    verify(device).setWebPush(null);
    verifyNoInteractions(fcmSender);
    verifyNoInteractions(apnSender);
    verifyNoInteractions(pushNotificationScheduler);
  }

  @Test
  void testSendNotificationUnregisteredApnTokenUpdated() {
    final Instant tokenTimestamp = Instant.now();

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final UUID aci = UUID.randomUUID();
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(device.getApnId()).thenReturn("apns-token");
    when(device.getPushTimestamp()).thenReturn(tokenTimestamp.toEpochMilli());
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
    when(account.getAccountIdentifier()).thenReturn(aci);
    when(accountsManager.getByAccountIdentifier(aci)).thenReturn(Optional.of(account));

    final PushNotification pushNotification = new PushNotification(
        new PushToken.APN("token"), PushNotification.NotificationType.NOTIFICATION, null, account, device, true, null);

    when(apnSender.sendNotification(pushNotification))
        .thenReturn(CompletableFuture.completedFuture(new SendPushNotificationResult(false, Optional.empty(), true, Optional.of(tokenTimestamp.minusSeconds(60)))));

    when(pushNotificationScheduler.cancelScheduledNotifications(account, device))
        .thenReturn(CompletableFuture.completedFuture(null));

    pushNotificationManager.sendNotification(pushNotification);

    verifyNoInteractions(fcmSender);
    verifyNoInteractions(webPushSender);
    verify(accountsManager, never()).updateDevice(eq(aci), eq(Device.PRIMARY_ID), any());
    verify(device, never()).setApnId(any());
    verify(pushNotificationScheduler, never()).cancelScheduledNotifications(account, device);
  }

  @Test
  void testHandleMessagesRetrieved() {
    final UUID accountIdentifier = UUID.randomUUID();
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final String userAgent = HttpHeaders.USER_AGENT;

    when(account.getAccountIdentifier()).thenReturn(accountIdentifier);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    when(pushNotificationScheduler.cancelScheduledNotifications(account, device))
        .thenReturn(CompletableFuture.completedFuture(null));

    pushNotificationManager.handleMessagesRetrieved(account, device, userAgent);

    verify(pushNotificationScheduler).cancelScheduledNotifications(account, device);
  }
}
