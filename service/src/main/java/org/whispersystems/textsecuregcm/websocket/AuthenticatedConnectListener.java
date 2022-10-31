/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer durationTimer = metricRegistry.timer(
      name(WebSocketConnection.class, "connected_duration"));
  private static final Timer unauthenticatedDurationTimer = metricRegistry.timer(
      name(WebSocketConnection.class, "unauthenticated_connection_duration"));
  private static final Counter openWebsocketCounter = metricRegistry.counter(
      name(WebSocketConnection.class, "open_websockets"));

  private static final String OPEN_WEBSOCKET_COUNTER_NAME = MetricsUtil.name(WebSocketConnection.class,
      "openWebsockets");

  private static final long RENEW_PRESENCE_INTERVAL_MINUTES = 5;

  private static final String REACTIVE_MESSAGE_QUEUE_EXPERIMENT_NAME = "reactive_message_queue_v1";

  private static final Logger log = LoggerFactory.getLogger(AuthenticatedConnectListener.class);

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final PushNotificationManager pushNotificationManager;
  private final ClientPresenceManager clientPresenceManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final AtomicInteger openReactiveWebSockets = new AtomicInteger(0);
  private final AtomicInteger openStandardWebSockets = new AtomicInteger(0);

  public AuthenticatedConnectListener(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      PushNotificationManager pushNotificationManager,
      ClientPresenceManager clientPresenceManager,
      ScheduledExecutorService scheduledExecutorService,
      ExperimentEnrollmentManager experimentEnrollmentManager) {
    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
    this.clientPresenceManager = clientPresenceManager;
    this.scheduledExecutorService = scheduledExecutorService;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    Metrics.gauge(OPEN_WEBSOCKET_COUNTER_NAME, Tags.of("reactive", String.valueOf(true)), openReactiveWebSockets);
    Metrics.gauge(OPEN_WEBSOCKET_COUNTER_NAME, Tags.of("reactive", String.valueOf(false)), openStandardWebSockets);
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    if (context.getAuthenticated() != null) {
      final AuthenticatedAccount auth = context.getAuthenticated(AuthenticatedAccount.class);
      final Device device = auth.getAuthenticatedDevice();
      final Timer.Context timer = durationTimer.time();
      final boolean enrolledInReactiveMessageQueue = experimentEnrollmentManager.isEnrolled(
          auth.getAccount().getUuid(),
          REACTIVE_MESSAGE_QUEUE_EXPERIMENT_NAME);
      final WebSocketConnection connection = new WebSocketConnection(receiptSender,
          messagesManager, auth, device,
          context.getClient(),
          scheduledExecutorService,
          enrolledInReactiveMessageQueue);

      openWebsocketCounter.inc();
      if (enrolledInReactiveMessageQueue) {
        openReactiveWebSockets.incrementAndGet();
      } else {
        openStandardWebSockets.incrementAndGet();
      }

      pushNotificationManager.handleMessagesRetrieved(auth.getAccount(), device, context.getClient().getUserAgent());

      final AtomicReference<ScheduledFuture<?>> renewPresenceFutureReference = new AtomicReference<>();

      context.addListener((closingContext, statusCode, reason) -> {
        openWebsocketCounter.dec();
        if (enrolledInReactiveMessageQueue) {
          openReactiveWebSockets.decrementAndGet();
        } else {
          openStandardWebSockets.decrementAndGet();
        }

        timer.stop();

        final ScheduledFuture<?> renewPresenceFuture = renewPresenceFutureReference.get();

        if (renewPresenceFuture != null) {
          renewPresenceFuture.cancel(false);
        }

        connection.stop();

        RedisOperation.unchecked(
            () -> clientPresenceManager.clearPresence(auth.getAccount().getUuid(), device.getId()));
        RedisOperation.unchecked(() -> {
          messagesManager.removeMessageAvailabilityListener(connection);

          if (messagesManager.hasCachedMessages(auth.getAccount().getUuid(), device.getId())) {
            try {
              pushNotificationManager.sendNewMessageNotification(auth.getAccount(), device.getId(), true);
            } catch (NotPushRegisteredException ignored) {
            }
          }
        });
      });

      try {
        connection.start();
        clientPresenceManager.setPresent(auth.getAccount().getUuid(), device.getId(), connection);
        messagesManager.addMessageAvailabilityListener(auth.getAccount().getUuid(), device.getId(), connection);

        renewPresenceFutureReference.set(scheduledExecutorService.scheduleAtFixedRate(() -> RedisOperation.unchecked(() ->
                clientPresenceManager.renewPresence(auth.getAccount().getUuid(), device.getId())),
            RENEW_PRESENCE_INTERVAL_MINUTES,
            RENEW_PRESENCE_INTERVAL_MINUTES,
            TimeUnit.MINUTES));
      } catch (final Exception e) {
        log.warn("Failed to initialize websocket", e);
        context.getClient().close(1011, "Unexpected error initializing connection");
      }
    } else {
      final Timer.Context timer = unauthenticatedDurationTimer.time();
      context.addListener((context1, statusCode, reason) -> timer.stop());
    }
  }
}
