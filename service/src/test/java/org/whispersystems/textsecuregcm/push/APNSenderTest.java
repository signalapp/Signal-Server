/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.eatthepath.pushy.apns.ApnsClient;
import com.eatthepath.pushy.apns.ApnsPushNotification;
import com.eatthepath.pushy.apns.DeliveryPriority;
import com.eatthepath.pushy.apns.PushNotificationResponse;
import com.eatthepath.pushy.apns.PushType;
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification;
import com.eatthepath.pushy.apns.util.concurrent.PushNotificationFuture;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

class APNSenderTest {

  private static final String DESTINATION_DEVICE_TOKEN = RandomStringUtils.secure().nextAlphanumeric(32);
  private static final String BUNDLE_ID = "org.signal.test";

  private Account destinationAccount;
  private Device destinationDevice;

  private ApnsClient apnsClient;
  private APNSender apnSender;

  @BeforeEach
  void setup() {
    destinationAccount = mock(Account.class);
    destinationDevice = mock(Device.class);

    apnsClient = mock(ApnsClient.class);
    apnSender = new APNSender(new SynchronousExecutorService(), apnsClient, BUNDLE_ID);

    when(destinationAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(destinationDevice));
    when(destinationDevice.getApnId()).thenReturn(DESTINATION_DEVICE_TOKEN);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendApns(final boolean urgent) {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer(
            (Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    PushNotification pushNotification = new PushNotification(DESTINATION_DEVICE_TOKEN, PushNotification.TokenType.APN,
        PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice, urgent);

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_DEVICE_TOKEN);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload())
        .isEqualTo(urgent ? APNSender.APN_NSE_NOTIFICATION_PAYLOAD : APNSender.APN_BACKGROUND_PAYLOAD);

    assertThat(notification.getValue().getPriority())
        .isEqualTo(urgent ? DeliveryPriority.IMMEDIATE : DeliveryPriority.CONSERVE_POWER);

    assertThat(notification.getValue().getTopic()).isEqualTo(BUNDLE_ID);
    assertThat(notification.getValue().getPushType())
        .isEqualTo(urgent ? PushType.ALERT : PushType.BACKGROUND);

    if (urgent) {
      assertThat(notification.getValue().getCollapseId()).isNotNull();
    } else {
      assertThat(notification.getValue().getCollapseId()).isNull();
    }

    assertThat(result.accepted()).isTrue();
    assertThat(result.errorCode()).isEmpty();
    assertThat(result.unregistered()).isFalse();

    verifyNoMoreInteractions(apnsClient);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Unregistered", "BadDeviceToken", "ExpiredToken"})
  void testUnregisteredUser(final String rejectionReason) {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of(rejectionReason));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer(
            (Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    PushNotification pushNotification = new PushNotification(DESTINATION_DEVICE_TOKEN, PushNotification.TokenType.APN,
        PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice, true);

    when(destinationDevice.getApnId()).thenReturn(DESTINATION_DEVICE_TOKEN);
    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(11));

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_DEVICE_TOKEN);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_NSE_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(result.accepted()).isFalse();
    assertThat(result.errorCode()).hasValue(rejectionReason);
    assertThat(result.unregistered()).isTrue();
  }

  @Test
  void testGenericFailure() {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("BadTopic"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer(
            (Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    PushNotification pushNotification = new PushNotification(DESTINATION_DEVICE_TOKEN, PushNotification.TokenType.APN,
        PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice, true);

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_DEVICE_TOKEN);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_NSE_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(result.accepted()).isFalse();
    assertThat(result.errorCode()).hasValue("BadTopic");
    assertThat(result.unregistered()).isFalse();
  }

  @Test
  void testFailure() {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0),
            new IOException("lost connection")));

    PushNotification pushNotification = new PushNotification(DESTINATION_DEVICE_TOKEN, PushNotification.TokenType.APN,
        PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice, true);

    assertThatThrownBy(() -> apnSender.sendNotification(pushNotification).join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(IOException.class);

    verify(apnsClient).sendNotification(any());

    verifyNoMoreInteractions(apnsClient);
  }

  private static class MockPushNotificationFuture<P extends ApnsPushNotification, V> extends
      PushNotificationFuture<P, V> {

    MockPushNotificationFuture(final P pushNotification, final V response) {
      super(pushNotification);
      complete(response);
    }

    MockPushNotificationFuture(final P pushNotification, final Exception exception) {
      super(pushNotification);
      completeExceptionally(exception);
    }
  }

}
