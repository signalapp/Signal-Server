/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.dropwizard.lifecycle.Managed;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient.ApnResult;

public class APNSender implements Managed, PushNotificationSender {

  private final ExecutorService    executor;
  private final String             bundleId;
  private final boolean            sandbox;
  private final RetryingApnsClient apnsClient;

  @VisibleForTesting
  static final String APN_VOIP_NOTIFICATION_PAYLOAD    = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}}";

  @VisibleForTesting
  static final String APN_NSE_NOTIFICATION_PAYLOAD     = "{\"aps\":{\"mutable-content\":1,\"alert\":{\"loc-key\":\"APN_Message\"}}}";

  @VisibleForTesting
  static final String APN_CHALLENGE_PAYLOAD            = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}, \"challenge\" : \"%s\"}";

  @VisibleForTesting
  static final String APN_RATE_LIMIT_CHALLENGE_PAYLOAD = "{\"aps\":{\"sound\":\"default\",\"alert\":{\"loc-key\":\"APN_Message\"}}, \"rateLimitChallenge\" : \"%s\"}";

  @VisibleForTesting
  static final Instant MAX_EXPIRATION = Instant.ofEpochMilli(Integer.MAX_VALUE * 1000L);

  public APNSender(ExecutorService executor, ApnConfiguration configuration)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException
  {
    this.executor        = executor;
    this.bundleId        = configuration.getBundleId();
    this.sandbox         = configuration.isSandboxEnabled();
    this.apnsClient      = new RetryingApnsClient(configuration.getSigningKey(),
                                                  configuration.getTeamId(),
                                                  configuration.getKeyId(),
                                                  sandbox);
  }

  @VisibleForTesting
  public APNSender(ExecutorService executor, RetryingApnsClient apnsClient, String bundleId, boolean sandbox) {
    this.executor        = executor;
    this.apnsClient      = apnsClient;
    this.sandbox         = sandbox;
    this.bundleId        = bundleId;
  }

  @Override
  public CompletableFuture<SendPushNotificationResult> sendNotification(final PushNotification notification) {
    final String topic = switch (notification.tokenType()) {
      case APN -> bundleId;
      case APN_VOIP -> bundleId + ".voip";
      default -> throw new IllegalArgumentException("Unsupported token type: " + notification.tokenType());
    };

    final boolean isVoip = notification.tokenType() == PushNotification.TokenType.APN_VOIP;

    final String payload = switch (notification.notificationType()) {
      case NOTIFICATION -> isVoip ? APN_VOIP_NOTIFICATION_PAYLOAD : APN_NSE_NOTIFICATION_PAYLOAD;
      case CHALLENGE -> String.format(APN_CHALLENGE_PAYLOAD, notification.data());
      case RATE_LIMIT_CHALLENGE -> String.format(APN_RATE_LIMIT_CHALLENGE_PAYLOAD, notification.data());
    };

    final String collapseId =
        (notification.notificationType() == PushNotification.NotificationType.NOTIFICATION && !isVoip)
            ? "incoming-message" : null;

    final ListenableFuture<ApnResult> sendFuture = apnsClient.send(notification.deviceToken(),
        topic,
        payload,
        MAX_EXPIRATION,
        isVoip,
        collapseId);

    final CompletableFuture<SendPushNotificationResult> completableSendFuture = new CompletableFuture<>();

    Futures.addCallback(sendFuture, new FutureCallback<>() {
      @Override
      public void onSuccess(@Nullable ApnResult result) {
        if (result == null) {
          // This should never happen
          completableSendFuture.completeExceptionally(new NullPointerException("apnResult was null"));
        } else {
          completableSendFuture.complete(switch (result.getStatus()) {
            case SUCCESS -> new SendPushNotificationResult(true, null, false);
            case NO_SUCH_USER -> new SendPushNotificationResult(false, result.getReason(), true);
            case GENERIC_FAILURE -> new SendPushNotificationResult(false, result.getReason(), false);
          });
        }
      }

      @Override
      public void onFailure(@Nullable Throwable t) {
        completableSendFuture.completeExceptionally(t);
      }
    }, executor);

    return completableSendFuture;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
    this.apnsClient.disconnect();
  }
}
