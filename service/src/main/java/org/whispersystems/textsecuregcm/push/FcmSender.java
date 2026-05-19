/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.GoogleApiUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class FcmSender implements PushNotificationSender {

  private final ExecutorService executor;
  private final FirebaseMessaging firebaseMessagingClient;

  // See https://firebase.google.com/docs/cloud-messaging/customize-messages/setting-message-lifespan
  private static final long DEFAULT_TTL_MILLIS = Duration.ofDays(28).toMillis();

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
            .setPriority(pushNotification.urgent() ? AndroidConfig.Priority.HIGH : AndroidConfig.Priority.NORMAL)
            .setTtl(pushNotification.ttl() != null ? pushNotification.ttl().toMillis() : DEFAULT_TTL_MILLIS)
            .build());

    final String key = switch (pushNotification.notificationType()) {
      case NOTIFICATION -> "newMessageAlert";
      case ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY -> "attemptLoginContext";
      case CHALLENGE -> "challenge";
      case RATE_LIMIT_CHALLENGE -> "rateLimitChallenge";
      case VERIFICATION_CODE_REQUESTED -> "verificationCodeRequested";
    };

    final String data = switch (pushNotification.notificationType()) {
      case VERIFICATION_CODE_REQUESTED -> {
        if (!(pushNotification.data() instanceof VerificationCodeRequestData)) {
          throw new IllegalArgumentException("Notification did not have VerificationCodeRequestData");
        }

        try {
          yield SystemMapper.jsonMapper().writeValueAsString(pushNotification.data());
        } catch (final JsonProcessingException e) {
          throw new UncheckedIOException(e);
        }
      }
      default -> pushNotification.data() != null ? pushNotification.data().toString() : "";
    };

    builder.putData(key, data);

    final Timer.Sample sample = Timer.start();

    return GoogleApiUtil.toCompletableFuture(firebaseMessagingClient.sendAsync(builder.build()), executor)
        .whenComplete((ignored, throwable) -> sample.stop(SEND_NOTIFICATION_TIMER))
        .thenApply(ignored -> new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty()))
        .exceptionally(ExceptionUtils.exceptionallyHandler(FirebaseMessagingException.class,
            firebaseMessagingException -> {
              final String errorCode;

              if (firebaseMessagingException.getMessagingErrorCode() != null) {
                errorCode = firebaseMessagingException.getMessagingErrorCode().name();
              } else if (firebaseMessagingException.getHttpResponse() != null) {
                errorCode = "http" + firebaseMessagingException.getHttpResponse().getStatusCode();
              } else {
                logger.warn("Received an FCM exception with no error code", firebaseMessagingException);
                errorCode = "unknown";
              }

              final boolean unregistered =
                  firebaseMessagingException.getMessagingErrorCode() == MessagingErrorCode.UNREGISTERED;

              return new SendPushNotificationResult(false, Optional.of(errorCode), unregistered, Optional.empty());
            }));
  }
}

