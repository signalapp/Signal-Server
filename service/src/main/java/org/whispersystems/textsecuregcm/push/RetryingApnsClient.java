/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.eatthepath.pushy.apns.ApnsClient;
import com.eatthepath.pushy.apns.ApnsClientBuilder;
import com.eatthepath.pushy.apns.DeliveryPriority;
import com.eatthepath.pushy.apns.PushNotificationResponse;
import com.eatthepath.pushy.apns.PushType;
import com.eatthepath.pushy.apns.auth.ApnsSigningKey;
import com.eatthepath.pushy.apns.metrics.dropwizard.DropwizardApnsClientMetricsListener;
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.codahale.metrics.MetricRegistry.name;

public class RetryingApnsClient {

  private static final Logger logger = LoggerFactory.getLogger(RetryingApnsClient.class);

  private final ApnsClient apnsClient;

  RetryingApnsClient(String apnSigningKey, String teamId, String keyId, boolean sandbox)
      throws IOException, InvalidKeyException, NoSuchAlgorithmException
  {
    MetricRegistry                      metricRegistry  = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    DropwizardApnsClientMetricsListener metricsListener = new DropwizardApnsClientMetricsListener();

    for (Map.Entry<String, Metric> entry : metricsListener.getMetrics().entrySet()) {
      metricRegistry.register(name(getClass(), entry.getKey()), entry.getValue());
    }

    this.apnsClient = new ApnsClientBuilder().setSigningKey(ApnsSigningKey.loadFromInputStream(new ByteArrayInputStream(apnSigningKey.getBytes()), teamId, keyId))
                                             .setMetricsListener(metricsListener)
                                             .setApnsServer(sandbox ? ApnsClientBuilder.DEVELOPMENT_APNS_HOST : ApnsClientBuilder.PRODUCTION_APNS_HOST)
                                             .build();
  }

  @VisibleForTesting
  public RetryingApnsClient(ApnsClient apnsClient) {
    this.apnsClient = apnsClient;
  }

  ListenableFuture<ApnResult> send(final String apnId, final String topic, final String payload, final Instant expiration, final boolean isVoip) {
    SettableFuture<ApnResult>  result       = SettableFuture.create();
    SimpleApnsPushNotification notification = new SimpleApnsPushNotification(apnId, topic, payload, expiration, DeliveryPriority.IMMEDIATE, isVoip ? PushType.VOIP : PushType.ALERT);
        
    apnsClient.sendNotification(notification).whenComplete(new ResponseHandler(result));

    return result;
  }

  void disconnect() {
    apnsClient.close();
  }

  private static final class ResponseHandler implements BiConsumer<PushNotificationResponse<SimpleApnsPushNotification>, Throwable> {

    private final SettableFuture<ApnResult> future;

    private ResponseHandler(SettableFuture<ApnResult> future) {
      this.future = future;
    }

    @Override
    public void accept(final PushNotificationResponse<SimpleApnsPushNotification> response, final Throwable cause) {
      if (response != null) {
        if (response.isAccepted()) {
          future.set(new ApnResult(ApnResult.Status.SUCCESS, null));
        } else if ("Unregistered".equals(response.getRejectionReason()) ||
                "BadDeviceToken".equals(response.getRejectionReason())) {
          future.set(new ApnResult(ApnResult.Status.NO_SUCH_USER, response.getRejectionReason()));
        } else {
          logger.warn("Got APN failure: " + response.getRejectionReason());
          future.set(new ApnResult(ApnResult.Status.GENERIC_FAILURE, response.getRejectionReason()));
        }
      } else {
        logger.warn("Execution exception", cause);
        future.setException(cause);
      }
    }
  }

  public static class ApnResult {
    public enum Status {
      SUCCESS, NO_SUCH_USER, GENERIC_FAILURE
    }

    private final Status status;
    private final String reason;

    ApnResult(Status status, String reason) {
      this.status = status;
      this.reason = reason;
    }

    public Status getStatus() {
      return status;
    }

    public String getReason() {
      return reason;
    }
  }

}
