/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

public class PushNotificationManager {

  private final AccountsManager accountsManager;
  private final APNSender apnSender;
  private final FcmSender fcmSender;
  private final WebPushSender webPushSender;
  private final PushNotificationScheduler pushNotificationScheduler;

  private static final Duration VERIFICATION_CODE_TTL = Duration.ofMinutes(10);

  private static final String SENT_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "sentPushNotification");
  private static final String FAILED_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "failedPushNotification");
  private static final String DEVICE_TOKEN_UNREGISTERED_COUNTER_NAME = name(PushNotificationManager.class, "deviceTokenUnregistered");

  private static final Logger logger = LoggerFactory.getLogger(PushNotificationManager.class);

  public PushNotificationManager(final AccountsManager accountsManager,
      final APNSender apnSender,
      final FcmSender fcmSender,
      final WebPushSender webPushSender,
      final PushNotificationScheduler pushNotificationScheduler) {

    this.accountsManager = accountsManager;
    this.apnSender = apnSender;
    this.fcmSender = fcmSender;
    this.webPushSender = webPushSender;
    this.pushNotificationScheduler = pushNotificationScheduler;
  }

  public CompletableFuture<Optional<SendPushNotificationResult>> sendNewMessageNotification(final Account destination, final byte destinationDeviceId, final boolean urgent) throws NotPushRegisteredException {
    final Device device = destination.getDevice(destinationDeviceId).orElseThrow(NotPushRegisteredException::new);
    final PushNotification.PushToken<?> token = Device.getPushToken(device);

    return sendNotification(new PushNotification(token,
        PushNotification.NotificationType.NOTIFICATION, null, destination, device, urgent, null));
  }

  /** To activate web push subscription */
  public CompletableFuture<SendPushNotificationResult> sendActivationTokenNotification(final PushNotification.PushToken<?> deviceToken, final String token) {
    return sendNotification(new PushNotification(deviceToken, PushNotification.NotificationType.ACTIVATION_TOKEN, token, null, null, true, null))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendRegistrationChallengeNotification(final PushNotification.PushToken<?> deviceToken, final String challengeToken) {
    return sendNotification(new PushNotification(deviceToken, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null, true,
        null))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendRateLimitChallengeNotification(final Account destination, final String challengeToken)
      throws NotPushRegisteredException {

    final Device device = destination.getPrimaryDevice();
    final PushNotification.PushToken<?> token = Device.getPushToken(device);

    return sendNotification(new PushNotification(token,
        PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, destination, device, true, null))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendAttemptLoginNotification(final Account destination, final String context) throws NotPushRegisteredException {
    final Device device = destination.getDevice(Device.PRIMARY_ID).orElseThrow(NotPushRegisteredException::new);
    final PushNotification.PushToken<?> token = Device.getPushToken(device);
    return sendNotification(new PushNotification(token,
        PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
        context, destination, device, true, null))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(() -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public CompletableFuture<SendPushNotificationResult> sendVerificationCodeRequestedNotifications(final Account destination, final Instant requestTimestamp)
      throws NotPushRegisteredException {

    final PushNotification.PushToken<?> token = Device.getPushToken(destination.getPrimaryDevice());
    return sendNotification(new PushNotification(token,
        PushNotification.NotificationType.VERIFICATION_CODE_REQUESTED,
        new VerificationCodeRequestData(requestTimestamp.toEpochMilli()),
        destination,
        destination.getPrimaryDevice(),
        true,
        VERIFICATION_CODE_TTL))
        .thenApply(maybeResponse -> maybeResponse.orElseThrow(
            () -> new AssertionError("Responses must be present for urgent notifications")));
  }

  public void handleMessagesRetrieved(final Account account, final Device device, final String userAgent) {
    pushNotificationScheduler.cancelScheduledNotifications(account, device).whenComplete(logErrors());
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
      case WEBPUSH -> webPushSender;
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
        logger.debug("Failed to deliver {} push notification to token of type {}",
            pushNotification.notificationType(), pushNotification.tokenType(),
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
    final PushNotification.PushToken<?> originalToken = Device.getPushToken(device, tokenType);

    if (originalToken.isBlank()) {
      return;
    }

    // Reread the account to avoid marking the caller's account as stale. The consumers of this class tend to
    // promise not to modify accounts. There's no need to force the caller to be considered mutable just for
    // updating an uninstalled feedback timestamp though.
    accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).ifPresent(rereadAccount ->
        rereadAccount.getDevice(device.getId()).ifPresent(rereadDevice ->
            accountsManager.updateDevice(rereadAccount.getAccountIdentifier(), device.getId(), d -> {
              // Don't clear the token if it's already changed
              if (originalToken.equals(Device.getPushToken(d, tokenType))) {
                switch (tokenType) {
                  case WEBPUSH -> {
                    d.setWebPush(null);
                    d.setWebPushActivation(null);
                  }
                  case FCM -> d.setGcmId(null);
                  case APN -> d.setApnId(null);
                }
              }
            })));
  }
}
