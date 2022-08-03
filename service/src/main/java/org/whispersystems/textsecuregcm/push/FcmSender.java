/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.MessagingErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class FcmSender implements PushNotificationSender {

  private final ExecutorService executor;
  private final FirebaseMessaging firebaseMessagingClient;

  private static final Logger logger = LoggerFactory.getLogger(FcmSender.class);

  public FcmSender(ExecutorService executor, String credentials) throws IOException {
    try (final ByteArrayInputStream credentialInputStream = new ByteArrayInputStream(credentials.getBytes(StandardCharsets.UTF_8))) {
      FirebaseOptions options = FirebaseOptions.builder()
          .setCredentials(GoogleCredentials.fromStream(credentialInputStream))
          .build();

      FirebaseApp.initializeApp(options);
    }

    this.executor = executor;
    this.firebaseMessagingClient = FirebaseMessaging.getInstance();
  }

  @VisibleForTesting
  public FcmSender(ExecutorService executor, FirebaseMessaging firebaseMessagingClient) {
    this.executor = executor;
    this.firebaseMessagingClient = firebaseMessagingClient;
  }

  @Override
  public CompletableFuture<SendPushNotificationResult> sendNotification(PushNotification pushNotification) {
    Message.Builder builder = Message.builder()
        .setToken(pushNotification.deviceToken())
        .setAndroidConfig(AndroidConfig.builder()
            .setPriority(AndroidConfig.Priority.HIGH)
            .build());

    final String key = switch (pushNotification.notificationType()) {
      case NOTIFICATION -> "notification";
      case CHALLENGE -> "challenge";
      case RATE_LIMIT_CHALLENGE -> "rateLimitChallenge";
    };

    builder.putData(key, pushNotification.data() != null ? pushNotification.data() : "");

    final ApiFuture<String> sendFuture = firebaseMessagingClient.sendAsync(builder.build());
    final CompletableFuture<SendPushNotificationResult> completableSendFuture = new CompletableFuture<>();

    sendFuture.addListener(() -> {
      try {
        sendFuture.get();
        completableSendFuture.complete(new SendPushNotificationResult(true, null, false));
      } catch (ExecutionException e) {
        if (e.getCause() instanceof final FirebaseMessagingException firebaseMessagingException) {
          final String errorCode;

          if (firebaseMessagingException.getMessagingErrorCode() != null) {
            errorCode = firebaseMessagingException.getMessagingErrorCode().name();
          } else {
            logger.warn("Received an FCM exception with no error code", firebaseMessagingException);
            errorCode = "unknown";
          }

          completableSendFuture.complete(new SendPushNotificationResult(false,
              errorCode,
              firebaseMessagingException.getMessagingErrorCode() == MessagingErrorCode.UNREGISTERED));
        } else {
          completableSendFuture.completeExceptionally(e.getCause());
        }
      } catch (InterruptedException e) {
        // This should never happen; by definition, if we're in the future's listener, the future is done, and so
        // `get()` should return immediately.
        completableSendFuture.completeExceptionally(e);
      }
    }, executor);

    return completableSendFuture;
  }
}
