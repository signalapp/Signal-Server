/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.ThreadManager;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.MessagingErrorCode;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FcmSender implements PushNotificationSender {

  private final ExecutorService executor;
  private final FirebaseMessaging firebaseMessagingClient;

  private static final Timer SEND_NOTIFICATION_TIMER = Metrics.timer(name(FcmSender.class, "sendNotification"));

  private static final Logger logger = LoggerFactory.getLogger(FcmSender.class);

  public FcmSender(ExecutorService executor, String credentials) throws IOException {
    try (final ByteArrayInputStream credentialInputStream = new ByteArrayInputStream(credentials.getBytes(StandardCharsets.UTF_8))) {
      FirebaseApp.initializeApp(FirebaseOptions.builder()
          .setCredentials(GoogleCredentials.fromStream(credentialInputStream))
          .setThreadManager(new ThreadManager() {
            @Override
            protected ExecutorService getExecutor(final FirebaseApp app) {
              return executor;
            }

            @Override
            protected void releaseExecutor(final FirebaseApp app, final ExecutorService executor) {
              // Do nothing; the executor service is managed by Dropwizard
            }

            @Override
            protected ThreadFactory getThreadFactory() {
              return new ThreadFactoryBuilder()
                  .setNameFormat("firebase-%d")
                  .build();
            }
          })
          .build());
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

    final Instant start = Instant.now();
    final CompletableFuture<SendPushNotificationResult> completableSendFuture = new CompletableFuture<>();

    final ApiFuture<String> sendFuture = firebaseMessagingClient.sendAsync(builder.build());

    // We want to record the time taken to send the push notification as directly as possible; executing this very small
    // bit of non-blocking measurement on the sender thread lets us do that without picking up any confounding factors
    // like having a callback waiting in an executor's queue.
    sendFuture.addListener(() -> SEND_NOTIFICATION_TIMER.record(Duration.between(start, Instant.now())),
        MoreExecutors.directExecutor());

    ApiFutures.addCallback(sendFuture, new ApiFutureCallback<>() {
      @Override
      public void onSuccess(final String result) {
        completableSendFuture.complete(new SendPushNotificationResult(true, null, false));
      }

      @Override
      public void onFailure(final Throwable cause) {
        if (cause instanceof final FirebaseMessagingException firebaseMessagingException) {
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
          completableSendFuture.completeExceptionally(cause);
        }
      }
    }, executor);

    return completableSendFuture;
  }
}
