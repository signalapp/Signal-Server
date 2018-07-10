package org.whispersystems.textsecuregcm.tests.push;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.relayrides.pushy.apns.ApnsClient;
import com.relayrides.pushy.apns.ApnsServerException;
import com.relayrides.pushy.apns.ClientNotConnectedException;
import com.relayrides.pushy.apns.DeliveryPriority;
import com.relayrides.pushy.apns.PushNotificationResponse;
import com.relayrides.pushy.apns.util.SimpleApnsPushNotification;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient.ApnResult;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultPromise;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class APNSenderTest {

  private static final String DESTINATION_NUMBER = "+14151231234";
  private static final String DESTINATION_APN_ID = "foo";

  private final AccountsManager accountsManager = mock(AccountsManager.class);

  private final Account            destinationAccount = mock(Account.class);
  private final Device             destinationDevice  = mock(Device.class);
  private final ApnFallbackManager fallbackManager    = mock(ApnFallbackManager.class);

  private final DefaultEventExecutor executor = new DefaultEventExecutor();

  @Before
  public void setup() {
    when(destinationAccount.getDevice(1)).thenReturn(Optional.of(destinationDevice));
    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
    when(accountsManager.get(DESTINATION_NUMBER)).thenReturn(Optional.of(destinationAccount));
  }

  @Test
  public void testSendVoip() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
    result.setSuccess(response);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(result);

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);

    apnSender.setApnFallbackManager(fallbackManager);
    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
    assertThat(notification.getValue().getPayload()).isEqualTo("message");
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo.voip");

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.SUCCESS);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  public void testSendApns() throws Exception {
    ApnsClient apnsClient = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
    result.setSuccess(response);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(result);

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
    ApnMessage message = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", false, 30);
    APNSender apnSender = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
    assertThat(notification.getValue().getPayload()).isEqualTo("message");
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);
    assertThat(notification.getValue().getTopic()).isEqualTo("foo");

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.SUCCESS);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

//  @Test
//  public void testUnregisteredUser() throws Exception {
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
//    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
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

//  @Test
//  public void testRecentUnregisteredUser() throws Exception {
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
//    when(destinationDevice.getApnId()).thenReturn(DESTINATION_APN_ID);
//    when(destinationDevice.getPushTimestamp()).thenReturn(System.currentTimeMillis());
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
//    verify(destinationDevice, times(1)).getPushTimestamp();
//
//    verifyNoMoreInteractions(destinationDevice);
//    verifyNoMoreInteractions(destinationAccount);
//    verifyNoMoreInteractions(accountsManager);
//    verifyNoMoreInteractions(fallbackManager);
//  }

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
  public void testGenericFailure() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(false);
    when(response.getRejectionReason()).thenReturn("BadTopic");

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
    result.setSuccess(response);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(result);

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);
    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(1)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
    assertThat(notification.getValue().getPayload()).isEqualTo("message");
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.GENERIC_FAILURE);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  public void testTransientFailure() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
    result.setFailure(new ClientNotConnectedException("lost connection"));

    DefaultPromise<Void> connectedResult = new DefaultPromise<>(executor);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(result);

    when(apnsClient.getReconnectionFuture())
        .thenReturn(connectedResult);

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 10);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);

    Thread.sleep(1000);

    assertThat(sendFuture.isDone()).isFalse();

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> updatedResult = new DefaultPromise<>(executor);
    updatedResult.setSuccess(response);

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(updatedResult);

    connectedResult.setSuccess(null);

    ApnResult apnResult = sendFuture.get();

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(2)).sendNotification(notification.capture());
    verify(apnsClient, times(1)).getReconnectionFuture();

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
    assertThat(notification.getValue().getPayload()).isEqualTo("message");
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    assertThat(apnResult.getStatus()).isEqualTo(ApnResult.Status.SUCCESS);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

  @Test
  public void testPersistentTransientFailure() throws Exception {
    ApnsClient      apnsClient      = mock(ApnsClient.class);

    PushNotificationResponse<SimpleApnsPushNotification> response = mock(PushNotificationResponse.class);
    when(response.isAccepted()).thenReturn(true);

    DefaultPromise<PushNotificationResponse<SimpleApnsPushNotification>> result = new DefaultPromise<>(executor);
    result.setFailure(new ApnsServerException("apn servers suck again"));

    when(apnsClient.sendNotification(any(SimpleApnsPushNotification.class)))
        .thenReturn(result);

    RetryingApnsClient retryingApnsClient = new RetryingApnsClient(apnsClient, 3);
    ApnMessage         message            = new ApnMessage(DESTINATION_APN_ID, DESTINATION_NUMBER, 1, "message", true, 30);
    APNSender          apnSender          = new APNSender(new SynchronousExecutorService(), accountsManager, retryingApnsClient, "foo", false);
    apnSender.setApnFallbackManager(fallbackManager);

    ListenableFuture<ApnResult> sendFuture = apnSender.sendMessage(message);

    try {
      sendFuture.get();
      throw new AssertionError("future did not throw exception");
    } catch (Exception e) {
      // good
    }

    ArgumentCaptor<SimpleApnsPushNotification> notification = ArgumentCaptor.forClass(SimpleApnsPushNotification.class);
    verify(apnsClient, times(4)).sendNotification(notification.capture());

    assertThat(notification.getValue().getToken()).isEqualTo(DESTINATION_APN_ID);
    assertThat(notification.getValue().getExpiration()).isEqualTo(new Date(30));
    assertThat(notification.getValue().getPayload()).isEqualTo("message");
    assertThat(notification.getValue().getPriority()).isEqualTo(DeliveryPriority.IMMEDIATE);

    verifyNoMoreInteractions(apnsClient);
    verifyNoMoreInteractions(accountsManager);
    verifyNoMoreInteractions(fallbackManager);
  }

}
