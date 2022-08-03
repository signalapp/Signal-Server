/*
 * Copyright 2013-2022 Signal Messenger, LLC
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
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification;
import com.eatthepath.pushy.apns.util.concurrent.PushNotificationFuture;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

class APNSenderTest {

  private static final String DESTINATION_APN_ID = "foo";

  private Account destinationAccount;
  private Device destinationDevice;

  private ApnsClient apnsClient;

  @BeforeEach
  void setup() {
    destinationAccount = mock(Account.class);
    destinationDevice  = mock(Device.class);

    apnsClient = mock(ApnsClient.class);

    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
  }

  @Test
  void testSendVoip() {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    PushNotification   pushNotification   = new PushNotification(DESTINATION_APN_ID, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), retryingApnsClient, "foo", false);

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo.voip");

    assertThat(result.accepted()).isTrue();
    assertThat(result.errorCode()).isNull();
    assertThat(result.unregistered()).isFalse();

    verifyNoMoreInteractions(apnsClient);
  }

  @Test
  void testSendApns() {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    PushNotification   pushNotification   = new PushNotification(DESTINATION_APN_ID, PushNotification.TokenType.APN, PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), retryingApnsClient, "foo", false);

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_NSE_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo");

    assertThat(result.accepted()).isTrue();
    assertThat(result.errorCode()).isNull();
    assertThat(result.unregistered()).isFalse();

    verifyNoMoreInteractions(apnsClient);
  }

  @Test
  void testUnregisteredUser() throws Exception {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("Unregistered"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));


    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    PushNotification   pushNotification   = new PushNotification(DESTINATION_APN_ID, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), retryingApnsClient, "foo", false);

    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(11));

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(result.accepted()).isFalse();
    assertThat(result.errorCode()).isEqualTo("Unregistered");
    assertThat(result.unregistered()).isTrue();
  }

  @Test
  void testGenericFailure() {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("BadTopic"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    PushNotification   pushNotification   = new PushNotification(DESTINATION_APN_ID, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), retryingApnsClient, "foo", false);

    final SendPushNotificationResult result = apnSender.sendNotification(pushNotification).join();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(APNSender.MAX_EXPIRATION);
    assertThat(notification.getValue().getPayload()).isEqualTo(APNSender.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(result.accepted()).isFalse();
    assertThat(result.errorCode()).isEqualTo("BadTopic");
    assertThat(result.unregistered()).isFalse();
  }

  @Test
  void testFailure() {
    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), new IOException("lost connection")));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    PushNotification   pushNotification   = new PushNotification(DESTINATION_APN_ID, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, destinationAccount, destinationDevice);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), retryingApnsClient, "foo", false);

    assertThatThrownBy(() -> apnSender.sendNotification(pushNotification).join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(IOException.class);

    verify(apnsClient).sendNotification(any());

    verifyNoMoreInteractions(apnsClient);
  }

  private static class MockPushNotificationFuture <P extends ApnsPushNotification, V> extends PushNotificationFuture<P, V> {

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
