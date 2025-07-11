/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

public class PushNotificationManager {

  private final AccountsManager accountsManager;
  private final PushNotificationSender apnSender;
  private final PushNotificationSender fcmSender;
  private final PushNotificationScheduler pushNotificationScheduler;

  private static final String SENT_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "sentPushNotification");
  private static final String FAILED_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "failedPushNotification");
  private static final String DEVICE_TOKEN_UNREGISTERED_COUNTER_NAME = name(PushNotificationManager.class, "deviceTokenUnregistered");

  private static final Logger logger = LoggerFactory.getLogger(PushNotificationManager.class);

  public PushNotificationManager(final AccountsManager accountsManager,
      final PushNotificationSender apnSender,
      final PushNotificationSender fcmSender,
      final PushNotificationScheduler pushNotificationScheduler) {

    this.accountsManager = accountsManager;
    this.apnSender = apnSender;
    this.fcmSender = fcmSender;
    this.pushNotificationScheduler = pushNotificationScheduler;
  }

  public CompletableFuture<Optional<SendPushNotificationResult>> sendNewMessageNotification(final Account destination, final byte destinationDeviceId, final boolean urgent) throws NotPushRegisteredException {
    final Device device = destination.getDevice(destinationDeviceId).orElseThrow(NotPushRegisteredException::new);
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    return sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.NOTIFICATION, null, destination, device, urgent));
  }

  public CompletableFuture<SendPushNotificationResult> sendRegistrationChallengeNotification(final String deviceToken, final PushNotification.TokenType tokenType, final String challengeToken) {
    return sendNotification(new PushNotification(deviceToken, tokenType, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null, true))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendRateLimitChallengeNotification(final Account destination, final String challengeToken)
      throws NotPushRegisteredException {

    final Device device = destination.getPrimaryDevice();
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    return sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, destination, device, true))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendAttemptLoginNotification(final Account destination, final String context) throws NotPushRegisteredException {
    final Device device = destination.getDevice(Device.PRIMARY_ID).orElseThrow(NotPushRegisteredException::new);
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    return sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
        context, destination, device, true))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public void handleMessagesRetrieved(final Account account, final Device device, final String userAgent) {
    pushNotificationScheduler.cancelScheduledNotifications(account, device).whenComplete(logErrors());
  }

  @VisibleForTesting
  Pair<String, PushNotification.TokenType> getToken(final Device device) throws NotPushRegisteredException {
    final Pair<String, PushNotification.TokenType> tokenAndType;

    if (StringUtils.isNotBlank(device.getGcmId())) {
      tokenAndType = new Pair<>(device.getGcmId(), PushNotification.TokenType.FCM);
    } else if (StringUtils.isNotBlank(device.getApnId())) {
      tokenAndType = new Pair<>(device.getApnId(), PushNotification.TokenType.APN);
    } else {
      throw new NotPushRegisteredException();
    }

    return tokenAndType;
  }

  @VisibleForTesting
  CompletableFuture<Optional<SendPushNotificationResult>> sendNotification(final PushNotification pushNotification) {
    if (!pushNotification.urgent()) {
      // Schedule a notification for some time in the future (possibly even now!) rather than sending a notification
      // directly
      return pushNotificationScheduler
          .scheduleBackgroundNotification(pushNotification.tokenType(), pushNotification.destination(), pushNotification.destinationDevice())
          .whenComplete(logErrors())
          .thenApply(ignored -> Optional.<SendPushNotificationResult>empty())
          .toCompletableFuture();
    }

    final PushNotificationSender sender = switch (pushNotification.tokenType()) {
      case FCM -> fcmSender;
      case APN -> apnSender;
    };

    return sender.sendNotification(pushNotification).whenComplete((result, throwable) -> {
      if (throwable == null) {
        Tags tags = Tags.of("tokenType", pushNotification.tokenType().name(),
            "notificationType", pushNotification.notificationType().name(),
            "urgent", String.valueOf(pushNotification.urgent()),
            "accepted", String.valueOf(result.accepted()),
            "unregistered", String.valueOf(result.unregistered()));

        if (result.errorCode().isPresent()) {
          tags = tags.and("errorCode", result.errorCode().get());
        }

        Metrics.counter(SENT_NOTIFICATION_COUNTER_NAME, tags).increment();

        if (result.unregistered() && pushNotification.destination() != null
            && pushNotification.destinationDevice() != null) {

          handleDeviceUnregistered(pushNotification.destination(),
              pushNotification.destinationDevice(),
              pushNotification.tokenType(),
              result.errorCode(),
              result.unregisteredTimestamp());
        }
      } else {
        logger.debug("Failed to deliver {} push notification to {} ({})",
            pushNotification.notificationType(), pushNotification.deviceToken(), pushNotification.tokenType(),
            throwable);

        Metrics.counter(FAILED_NOTIFICATION_COUNTER_NAME, "cause", throwable.getClass().getSimpleName()).increment();
      }
    })
        .thenApply(Optional::of);
  }

  private static <T> BiConsumer<T, Throwable> logErrors() {
    return (ignored, throwable) -> {
      if (throwable != null) {
        logger.warn("Failed push scheduling operation", throwable);
      }
    };
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void handleDeviceUnregistered(final Account account,
      final Device device,
      final PushNotification.TokenType tokenType,
      final Optional<String> maybeErrorCode,
      final Optional<Instant> maybeTokenInvalidationTimestamp) {

    final boolean tokenExpired = maybeTokenInvalidationTimestamp.map(tokenInvalidationTimestamp ->
        tokenInvalidationTimestamp.isAfter(Instant.ofEpochMilli(device.getPushTimestamp()))).orElse(true);

    if (tokenExpired) {
      if (tokenType == PushNotification.TokenType.APN) {
        pushNotificationScheduler.cancelScheduledNotifications(account, device).whenComplete(logErrors());
      }

      clearPushToken(account, device, tokenType);
    }
    Metrics.counter(DEVICE_TOKEN_UNREGISTERED_COUNTER_NAME,
        "errorCode", maybeErrorCode.orElse("unknown"),
        "isPrimary", String.valueOf(device.isPrimary()),
        "hasUnregisteredTimestamp", String.valueOf(maybeTokenInvalidationTimestamp.isPresent()),
        "tokenType", tokenType.name(),
        "tokenExpired", String.valueOf(tokenExpired)).increment();
  }

  private void clearPushToken(final Account account, final Device device, final PushNotification.TokenType tokenType) {
    final String originalToken = getPushToken(device, tokenType);

    if (originalToken == null) {
      return;
    }

    // Reread the account to avoid marking the caller's account as stale. The consumers of this class tend to
    // promise not to modify accounts. There's no need to force the caller to be considered mutable just for
    // updating an uninstalled feedback timestamp though.
    accountsManager.getByAccountIdentifier(account.getUuid()).ifPresent(rereadAccount ->
        rereadAccount.getDevice(device.getId()).ifPresent(rereadDevice ->
            accountsManager.updateDevice(rereadAccount, device.getId(), d -> {
              // Don't clear the token if it's already changed
              if (originalToken.equals(getPushToken(d, tokenType))) {
                switch (tokenType) {
                  case FCM -> d.setGcmId(null);
                  case APN -> d.setApnId(null);
                }
              }
            })));
  }

  private static String getPushToken(final Device device, final PushNotification.TokenType tokenType) {
    return switch (tokenType) {
      case FCM -> device.getGcmId();
      case APN -> device.getApnId();
    };
  }
}
