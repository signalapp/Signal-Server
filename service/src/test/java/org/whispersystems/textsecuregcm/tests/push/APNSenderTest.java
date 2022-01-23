/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.push;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.eatthepath.pushy.apns.ApnsClient;
import com.eatthepath.pushy.apns.ApnsPushNotification;
import com.eatthepath.pushy.apns.DeliveryPriority;
import com.eatthepath.pushy.apns.PushNotificationResponse;
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification;
import com.eatthepath.pushy.apns.util.concurrent.PushNotificationFuture;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.ApnMessage.Type;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient.ApnResult;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

class APNSenderTest {

  private static final UUID DESTINATION_UUID = UUID.randomUUID();
  private static final String DESTINATION_APN_ID = "foo";

  private final AccountsManager accountsManager = mock(AccountsManager.class);

  private final Account            destinationAccount = mock(Account.class);
  private final Device             destinationDevice  = mock(Device.class);
  private final ApnFallbackManager fallbackManager    = mock(ApnFallbackManager.class);

  @BeforeEach
  void setup() {
    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(accountsManager.getByAccountIdentifier(DESTINATION_UUID)).thenReturn(Optional.of(destinationAccount));
  }

  @Test
  void testSendVoip() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, true, Type.NOTIFICATION, Optional.empty());
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);

    apnSender.setApnFallbackManager(fallbackManager);
    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo.voip");

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.SUCCESS);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  void testSendApns() throws Exception {
    ApnsClient apnsClient = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage message = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, false, Type.NOTIFICATION, Optional.empty());
    APNSender apnSender = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_NSE_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo");

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.SUCCESS);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  void testUnregisteredUser() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("Unregistered"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));


    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, true, Type.NOTIFICATION, Optional.empty());
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(11));

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    Thread.sleep(1000); // =(

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.NO_SUCH_USER);

    verifyNoMoreInteractions(apnsClient);
    verify(accountsManager, times(1)).getByAccountIdentifier(eq(DESTINATION_UUID));
    verify(destinationAccount, times(1)).getDevice(1);
    verify(destinationDevice, times(1)).getApnId();
    verify(destinationDevice, times(1)).getPushTimestamp();
//    verify(destinationDevice, times(1)).setApnId(eq((String)null));
//    verify(destinationDevice, times(1)).setVoipApnId(eq((String)null));
//    verify(destinationDevice, times(1)).setFetchesMessages(eq(false));
//    verify(accountsManager, times(1)).update(eq(destinationAccount));
    verify(fallbackManager, times(1)).cancel(eq(destinationAccount), eq(destinationDevice));

    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

//  @Test
//  public void testVoipUnregisteredUser() throws Exception {
//    ApnsClient      apnsClient      = mock(ApnsClient.class);
//
//    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
//    when(response.isAccepted()).thenReturn(false);
//    when(response.getRejectionReason()).thenReturn("Unregistered");
//
//    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
//    result.setSuccess(response);
//
//    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
//        .thenReturn(result);
//
//    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
//    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
//    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
//    apnSender.setApnFallbackManager(fallbackManager);
//
//    when(destinationDevice.getApnId()).thenReturn("baz");
//    when(destinationDevice.getVoipApnId()).thenReturn(DESTINATION_APN_ID);
//    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(11));
//
//    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
//    ApnResult apnResult = sendFuture.get();
//
//    Thread.sleep(1000); // =(
//
//    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
//    verify(apnsClient, times(1)).sendNotification(notification.capture());
//
//    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
//    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
//    assertThat(notification.getValue().getPayload()).isEqualTo("message");
//    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
//
//    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.NO_SUCH_USER);
//
//    verifyNoMoreInteractions(apnsClient);
//    verify(accountsManager, times(1)).get(eq(DESTINATION_NUMBER));
//    verify(destinationAccount, times(1)).getDevice(1);
//    verify(destinationDevice, times(1)).getApnId();
//    verify(destinationDevice, times(1)).getVoipApnId();
//    verify(destinationDevice, times(1)).getPushTimestamp();
//    verify(destinationDevice, times(1)).setApnId(eq((String)null));
//    verify(destinationDevice, times(1)).setVoipApnId(eq((String)null));
//    verify(destinationDevice, times(1)).setFetchesMessages(eq(false));
//    verify(accountsManager, times(1)).update(eq(destinationAccount));
//    verify(fallbackManager, times(1)).cancel(eq(new WebsocketAddress(DESTINATION_NUMBER, 1)));
//
//    verifyNoMoreInteractions(accountsManager);
//    verifyNoMoreInteractions(fallbackManager);
//  }

  @Test
  void testRecentUnregisteredUser() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("Unregistered"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, true, Type.NOTIFICATION, Optional.empty());
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis());

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    Thread.sleep(1000); // =(

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.NO_SUCH_USER);

    verifyNoMoreInteractions(apnsClient);
    verify(accountsManager, times(1)).getByAccountIdentifier(eq(DESTINATION_UUID));
    verify(destinationAccount, times(1)).getDevice(1);
    verify(destinationDevice, times(1)).getApnId();
    verify(destinationDevice, times(1)).getPushTimestamp();

    verifyNoMoreInteractions(destinationDevice);
    verifyNoMoreInteractions(destinationAccount);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

//  @Test
//  public void testUnregisteredUserOldApnId() throws Exception {
//    ApnsClient      apnsClient      = mock(ApnsClient.class);
//
//    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
//    when(response.isAccepted()).thenReturn(false);
//    when(response.getRejectionReason()).thenReturn("Unregistered");
//
//    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
//    result.setSuccess(response);
//
//    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
//        .thenReturn(result);
//
//    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
//    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
//    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
//    apnSender.setApnFallbackManager(fallbackManager);
//
//    when(destinationDevice.getApnId()).thenReturn("baz");
//    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(12));
//
//    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
//    ApnResult apnResult = sendFuture.get();
//
//    Thread.sleep(1000); // =(
//
//    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
//    verify(apnsClient, times(1)).sendNotification(notification.capture());
//
//    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
//    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
//    assertThat(notification.getValue().getPayload()).isEqualTo("message");
//    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
//
//    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.NO_SUCH_USER);
//
//    verifyNoMoreInteractions(apnsClient);
//    verify(accountsManager, times(1)).get(eq(DESTINATION_NUMBER));
//    verify(destinationAccount, times(1)).getDevice(1);
//    verify(destinationDevice, times(2)).getApnId();
//    verify(destinationDevice, times(2)).getVoipApnId();
//
//    verifyNoMoreInteractions(destinationDevice);
//    verifyNoMoreInteractions(destinationAccount);
//    verifyNoMoreInteractions(accountsManager);
//    verifyNoMoreInteractions(fallbackManager);
//  }

  @Test
  void testGenericFailure() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn(Optional.of("BadTopic"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), response));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, true, Type.NOTIFICATION, Optional.empty());
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.GENERIC_FAILURE);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  void testFailure() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenAnswer((Answer) invocationOnMock -> new MockPushNotificationFuture<>(invocationOnMock.getArgument(0), new Exception("lost connection")));

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_UUID, 1, true, Type.NOTIFICATION, Optional.empty());
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);

    try {
      sendFuture.get();
      throw new AssertionError();
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    } catch (ExecutionException e) {
      // good
    }

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(Instant.ofEpochMilli(ApnMessage.MAX_EXPIRATION));
    assertThat(notification.getValue().getPayload()).isEqualTo(ApnMessage.APN_VOIP_NOTIFICATION_PAYLOAD);
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
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
