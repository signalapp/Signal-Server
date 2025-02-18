/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.MessagingErrorCode;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.tests.util.SynchronousExecutorService;

class FcmSenderTest {

  private ExecutorService executorService;
  private FirebaseMessaging firebaseMessaging;

  private FcmSender fcmSender;

  @BeforeEach
  void setUp() {
    executorService = new SynchronousExecutorService();
    firebaseMessaging = mock(FirebaseMessaging.class);

    fcmSender = new FcmSender(executorService, firebaseMessaging);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executorService.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void testSendMessage() {
    final PushNotification pushNotification = new PushNotification("foo", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    final SettableApiFuture<String> sendFuture = SettableApiFuture.create();
    sendFuture.set("message-id");

    when(firebaseMessaging.sendAsync(any())).thenReturn(sendFuture);

    final SendPushNotificationResult result = fcmSender.sendNotification(pushNotification).join();

    verify(firebaseMessaging).sendAsync(any(Message.class));
    assertTrue(result.accepted());
    assertTrue(result.errorCode().isEmpty());
    assertFalse(result.unregistered());
  }

  @Test
  void testSendMessageRejected() {
    final PushNotification pushNotification = new PushNotification("foo", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    final FirebaseMessagingException invalidArgumentException = mock(FirebaseMessagingException.class);
    when(invalidArgumentException.getMessagingErrorCode()).thenReturn(MessagingErrorCode.INVALID_ARGUMENT);

    final SettableApiFuture<String> sendFuture = SettableApiFuture.create();
    sendFuture.setException(invalidArgumentException);

    when(firebaseMessaging.sendAsync(any())).thenReturn(sendFuture);

    final SendPushNotificationResult result = fcmSender.sendNotification(pushNotification).join();

    verify(firebaseMessaging).sendAsync(any(Message.class));
    assertFalse(result.accepted());
    assertEquals(Optional.of("INVALID_ARGUMENT"), result.errorCode());
    assertFalse(result.unregistered());
  }

  @Test
  void testSendMessageUnregistered() {
    final PushNotification pushNotification = new PushNotification("foo", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    final FirebaseMessagingException unregisteredException = mock(FirebaseMessagingException.class);
    when(unregisteredException.getMessagingErrorCode()).thenReturn(MessagingErrorCode.UNREGISTERED);

    final SettableApiFuture<String> sendFuture = SettableApiFuture.create();
    sendFuture.setException(unregisteredException);

    when(firebaseMessaging.sendAsync(any())).thenReturn(sendFuture);

    final SendPushNotificationResult result = fcmSender.sendNotification(pushNotification).join();

    verify(firebaseMessaging).sendAsync(any(Message.class));
    assertFalse(result.accepted());
    assertEquals(Optional.of("UNREGISTERED"), result.errorCode());
    assertTrue(result.unregistered());
  }

  @Test
  void testSendMessageException() {
    final PushNotification pushNotification = new PushNotification("foo", PushNotification.TokenType.FCM, PushNotification.NotificationType.NOTIFICATION, null, null, null, true);

    final SettableApiFuture<String> sendFuture = SettableApiFuture.create();
    sendFuture.setException(new IOException());

    when(firebaseMessaging.sendAsync(any())).thenReturn(sendFuture);

    final CompletionException completionException =
        assertThrows(CompletionException.class, () -> fcmSender.sendNotification(pushNotification).join());

    verify(firebaseMessaging).sendAsync(any(Message.class));
    assertTrue(completionException.getCause() instanceof IOException);
  }
}
