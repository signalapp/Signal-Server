/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

public class PushNotificationManager {

  private final AccountsManager accountsManager;
  private final APNSender apnSender;
  private final FcmSender fcmSender;
  private final ApnPushNotificationScheduler apnPushNotificationScheduler;
  private final PushLatencyManager pushLatencyManager;

  private static final String SENT_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "sentPushNotification");
  private static final String FAILED_NOTIFICATION_COUNTER_NAME = name(PushNotificationManager.class, "failedPushNotification");

  private static final Logger logger = LoggerFactory.getLogger(PushNotificationManager.class);

  public PushNotificationManager(final AccountsManager accountsManager,
      final APNSender apnSender,
      final FcmSender fcmSender,
      final ApnPushNotificationScheduler apnPushNotificationScheduler,
      final PushLatencyManager pushLatencyManager) {

    this.accountsManager = accountsManager;
    this.apnSender = apnSender;
    this.fcmSender = fcmSender;
    this.apnPushNotificationScheduler = apnPushNotificationScheduler;
    this.pushLatencyManager = pushLatencyManager;
  }

  public void sendNewMessageNotification(final Account destination, final byte destinationDeviceId, final boolean urgent) throws NotPushRegisteredException {
    final Device device = destination.getDevice(destinationDeviceId).orElseThrow(NotPushRegisteredException::new);
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.NOTIFICATION, null, destination, device, urgent));
  }

  public void sendRegistrationChallengeNotification(final String deviceToken, final PushNotification.TokenType tokenType, final String challengeToken) {
    sendNotification(new PushNotification(deviceToken, tokenType, PushNotification.NotificationType.CHALLENGE, challengeToken, null, null, true));
  }

  public void sendRateLimitChallengeNotification(final Account destination, final String challengeToken)
      throws NotPushRegisteredException {

    final Device device = destination.getPrimaryDevice();
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.RATE_LIMIT_CHALLENGE, challengeToken, destination, device, true));
  }

  public void sendAttemptLoginNotification(final Account destination, final String context) throws NotPushRegisteredException {
    final Device device = destination.getDevice(Device.PRIMARY_ID).orElseThrow(NotPushRegisteredException::new);
    final Pair<String, PushNotification.TokenType> tokenAndType = getToken(device);

    sendNotification(new PushNotification(tokenAndType.first(), tokenAndType.second(),
        PushNotification.NotificationType.ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
        context, destination, device, true));
  }

  public void handleMessagesRetrieved(final Account account, final Device device, final String userAgent) {
    RedisOperation.unchecked(() -> pushLatencyManager.recordQueueRead(account.getUuid(), device.getId(), userAgent));
    apnPushNotificationScheduler.cancelScheduledNotifications(account, device).whenComplete(logErrors());
  }

  @VisibleForTesting
  Pair<String, PushNotification.TokenType> getToken(final Device device) throws NotPushRegisteredException {
    final Pair<String, PushNotification.TokenType> tokenAndType;

    if (StringUtils.isNotBlank(device.getGcmId())) {
      tokenAndType = new Pair<>(device.getGcmId(), PushNotification.TokenType.FCM);
    } else if (StringUtils.isNotBlank(device.getVoipApnId())) {
      tokenAndType = new Pair<>(device.getVoipApnId(), PushNotification.TokenType.APN_VOIP);
    } else if (StringUtils.isNotBlank(device.getApnId())) {
      tokenAndType = new Pair<>(device.getApnId(), PushNotification.TokenType.APN);
    } else {
      throw new NotPushRegisteredException();
    }

    return tokenAndType;
  }

  @VisibleForTesting
  void sendNotification(final PushNotification pushNotification) {
    if (pushNotification.tokenType() == PushNotification.TokenType.APN && !pushNotification.urgent()) {
      // APNs imposes a per-device limit on background push notifications; schedule a notification for some time in the
      // future (possibly even now!) rather than sending a notification directly
      apnPushNotificationScheduler
          .scheduleBackgroundNotification(pushNotification.destination(), pushNotification.destinationDevice())
          .whenComplete(logErrors());

    } else {
      final PushNotificationSender sender = switch (pushNotification.tokenType()) {
        case FCM -> fcmSender;
        case APN, APN_VOIP -> apnSender;
      };

      sender.sendNotification(pushNotification).whenComplete((result, throwable) -> {
        if (throwable == null) {
          Tags tags = Tags.of("tokenType", pushNotification.tokenType().name(),
              "notificationType", pushNotification.notificationType().name(),
              "urgent", String.valueOf(pushNotification.urgent()),
              "accepted", String.valueOf(result.accepted()),
              "unregistered", String.valueOf(result.unregistered()));

          if (StringUtils.isNotBlank(result.errorCode())) {
            tags = tags.and("errorCode", result.errorCode());
          }

          Metrics.counter(SENT_NOTIFICATION_COUNTER_NAME, tags).increment();

          if (result.unregistered() && pushNotification.destination() != null
              && pushNotification.destinationDevice() != null) {
            handleDeviceUnregistered(pushNotification.destination(), pushNotification.destinationDevice());
          }

          if (result.accepted() &&
              pushNotification.tokenType() == PushNotification.TokenType.APN_VOIP &&
              pushNotification.notificationType() == PushNotification.NotificationType.NOTIFICATION &&
              pushNotification.destination() != null &&
              pushNotification.destinationDevice() != null) {

            apnPushNotificationScheduler.scheduleRecurringVoipNotification(
                    pushNotification.destination(),
                    pushNotification.destinationDevice())
                .whenComplete(logErrors());
          }
        } else {
          logger.debug("Failed to deliver {} push notification to {} ({})",
              pushNotification.notificationType(), pushNotification.deviceToken(), pushNotification.tokenType(),
              throwable);

          Metrics.counter(FAILED_NOTIFICATION_COUNTER_NAME, "cause", throwable.getClass().getSimpleName()).increment();
        }
      });
    }
  }

  private static <T> BiConsumer<T, Throwable> logErrors() {
    return (ignored, throwable) -> {
      if (throwable != null) {
        logger.warn("Failed push scheduling operation", throwable);
      }
    };
  }

  private void handleDeviceUnregistered(final Account account, final Device device) {
    if (StringUtils.isNotBlank(device.getGcmId())) {
      if (device.getUninstalledFeedbackTimestamp() == 0) {
        // Reread the account to avoid marking the caller's account as stale. The consumers of this class tend to
        // promise not to modify accounts. There's no need to force the caller to be considered mutable just for
        // updating an uninstalled feedback timestamp though.
        final Optional<Account> rereadAccount = accountsManager.getByAccountIdentifier(account.getUuid());
        if (rereadAccount.isEmpty()) {
          // don't bother adding the uninstalled timestamp, the account is gone
          return;
        }
        final Optional<Device> rereadDevice = rereadAccount.get().getDevice(device.getId());
        if (rereadDevice.map(Device::getUninstalledFeedbackTimestamp).orElse(-1L) != 0) {
          // don't bother adding the uninstalled timestamp, the device is gone or already updated
          return;
        }
        accountsManager.updateDevice(rereadAccount.get(), device.getId(), d ->
            d.setUninstalledFeedbackTimestamp(Util.todayInMillis()));
      }
    } else {
      apnPushNotificationScheduler.cancelScheduledNotifications(account, device).whenComplete(logErrors());
    }
  }
}
