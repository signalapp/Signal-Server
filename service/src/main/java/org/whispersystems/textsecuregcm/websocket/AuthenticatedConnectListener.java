/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;
import reactor.core.scheduler.Scheduler;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final String OPEN_WEBSOCKET_GAUGE_NAME = name(WebSocketConnection.class, "openWebsockets");
  private static final String CONNECTED_DURATION_TIMER_NAME =
      name(AuthenticatedConnectListener.class, "connectedDuration");

  private static final String AUTHENTICATED_TAG_NAME = "authenticated";

  private static final long RENEW_PRESENCE_INTERVAL_MINUTES = 5;

  private static final Logger log = LoggerFactory.getLogger(AuthenticatedConnectListener.class);

  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final MessageMetrics messageMetrics;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final ClientPresenceManager clientPresenceManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Scheduler messageDeliveryScheduler;
  private final ClientReleaseManager clientReleaseManager;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;

  private final Map<ClientPlatform, AtomicInteger> openAuthenticatedWebsocketsByClientPlatform;
  private final Map<ClientPlatform, AtomicInteger> openUnauthenticatedWebsocketsByClientPlatform;
  private final Map<ClientPlatform, Timer> durationTimersByClientPlatform;
  private final Map<ClientPlatform, Timer> unauthenticatedDurationTimersByClientPlatform;

  private final AtomicInteger openAuthenticatedWebsocketsFromUnknownPlatforms;
  private final AtomicInteger openUnauthenticatedWebsocketsFromUnknownPlatforms;
  private final Timer durationTimerForUnknownPlatforms;
  private final Timer unauthenticatedDurationTimerForUnknownPlatforms;

  public AuthenticatedConnectListener(ReceiptSender receiptSender,
      MessagesManager messagesManager,
      MessageMetrics messageMetrics,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      ClientPresenceManager clientPresenceManager,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler messageDeliveryScheduler,
      ClientReleaseManager clientReleaseManager,
      MessageDeliveryLoopMonitor messageDeliveryLoopMonitor) {
    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.messageMetrics = messageMetrics;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.clientPresenceManager = clientPresenceManager;
    this.scheduledExecutorService = scheduledExecutorService;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;

    openAuthenticatedWebsocketsByClientPlatform = new EnumMap<>(ClientPlatform.class);
    openUnauthenticatedWebsocketsByClientPlatform = new EnumMap<>(ClientPlatform.class);
    durationTimersByClientPlatform = new EnumMap<>(ClientPlatform.class);
    unauthenticatedDurationTimersByClientPlatform = new EnumMap<>(ClientPlatform.class);

    final Tags authenticatedTag = Tags.of(AUTHENTICATED_TAG_NAME, "true");
    final Tags unauthenticatedTag = Tags.of(AUTHENTICATED_TAG_NAME, "false");

    for (final ClientPlatform clientPlatform : ClientPlatform.values()) {
      openAuthenticatedWebsocketsByClientPlatform.put(clientPlatform, new AtomicInteger(0));
      openUnauthenticatedWebsocketsByClientPlatform.put(clientPlatform, new AtomicInteger(0));

      final Tags clientPlatformTag = Tags.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatform.name().toLowerCase());
      Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME, clientPlatformTag.and(authenticatedTag),
          openAuthenticatedWebsocketsByClientPlatform.get(clientPlatform));

      Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME, clientPlatformTag.and(unauthenticatedTag),
          openUnauthenticatedWebsocketsByClientPlatform.get(clientPlatform));

      durationTimersByClientPlatform.put(clientPlatform,
          Metrics.timer(CONNECTED_DURATION_TIMER_NAME, clientPlatformTag.and(authenticatedTag)));

      unauthenticatedDurationTimersByClientPlatform.put(clientPlatform,
          Metrics.timer(CONNECTED_DURATION_TIMER_NAME, clientPlatformTag.and(unauthenticatedTag)));
    }

    openAuthenticatedWebsocketsFromUnknownPlatforms = new AtomicInteger(0);
    openUnauthenticatedWebsocketsFromUnknownPlatforms = new AtomicInteger(0);

    final Tags unrecognizedPlatform = Tags.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized");
    Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME, unrecognizedPlatform.and(authenticatedTag),
        openAuthenticatedWebsocketsFromUnknownPlatforms);

    Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME, unrecognizedPlatform.and(unauthenticatedTag),
        openUnauthenticatedWebsocketsFromUnknownPlatforms);

    durationTimerForUnknownPlatforms = Metrics.timer(CONNECTED_DURATION_TIMER_NAME,
        unrecognizedPlatform.and(authenticatedTag));

    unauthenticatedDurationTimerForUnknownPlatforms = Metrics.timer(CONNECTED_DURATION_TIMER_NAME,
        unrecognizedPlatform.and(unauthenticatedTag));
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {

    final boolean authenticated = (context.getAuthenticated() != null);
    final String userAgent = context.getClient().getUserAgent();
    final AtomicInteger openWebsocketAtomicInteger = getOpenWebsocketCounter(userAgent, authenticated);
    final Timer connectionTimer = getConnectionTimer(userAgent, authenticated);

    if (authenticated) {
      final AuthenticatedDevice auth = context.getAuthenticated(AuthenticatedDevice.class);
      final Timer.Sample sample = Timer.start();
      final WebSocketConnection connection = new WebSocketConnection(receiptSender,
          messagesManager,
          messageMetrics,
          pushNotificationManager,
          pushNotificationScheduler,
          auth,
          context.getClient(),
          scheduledExecutorService,
          messageDeliveryScheduler,
          clientReleaseManager,
          messageDeliveryLoopMonitor);

      openWebsocketAtomicInteger.incrementAndGet();

      final AtomicReference<ScheduledFuture<?>> renewPresenceFutureReference = new AtomicReference<>();

      context.addWebsocketClosedListener((closingContext, statusCode, reason) -> {
        openWebsocketAtomicInteger.decrementAndGet();
        sample.stop(connectionTimer);

        final ScheduledFuture<?> renewPresenceFuture = renewPresenceFutureReference.get();

        if (renewPresenceFuture != null) {
          renewPresenceFuture.cancel(false);
        }

        // We begin the shutdown process by removing this client's "presence," which means it will again begin to
        // receive push notifications for inbound messages. We should do this first because, at this point, the
        // connection has already closed and attempts to actually deliver a message via the connection will not succeed.
        // It's preferable to start sending push notifications as soon as possible.
        RedisOperation.unchecked(() -> clientPresenceManager.clearPresence(auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId(), connection));

        // Next, we stop listening for inbound messages. If a message arrives after this call, the websocket connection
        // will not be notified and will not change its state, but that's okay because it has already closed and
        // attempts to deliver mesages via this connection will not succeed.
        RedisOperation.unchecked(() -> messagesManager.removeMessageAvailabilityListener(connection));

        // Finally, stop trying to deliver messages and send a push notification if the connection is aware of any
        // undelivered messages.
        connection.stop();
      });

      try {
        // Once we add this connection as a message availability listener, it will be notified any time a new message
        // arrives in the message cache. This updates the connection's "may have messages" state. It's important that
        // we do this first because we want to make sure we're accurately tracking message availability in the
        // connection's internal state.
        messagesManager.addMessageAvailabilityListener(auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId(), connection);

        // Once we "start" the websocket connection, we'll cancel any scheduled "you may have new messages" push
        // notifications and begin delivering any stored messages for the connected device. We have not yet declared the
        // client as "present" yet. If a message arrives at this point, we will update the message availability state
        // correctly, but we may also send a spurious push notification.
        connection.start();

        // Finally, we register this client's presence, which suppresses push notifications. We do this last because
        // receiving extra push notifications is generally preferable to missing out on a push notification.
        clientPresenceManager.setPresent(auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId(), connection);

        renewPresenceFutureReference.set(scheduledExecutorService.scheduleAtFixedRate(() -> RedisOperation.unchecked(() ->
                clientPresenceManager.renewPresence(auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId())),
            RENEW_PRESENCE_INTERVAL_MINUTES,
            RENEW_PRESENCE_INTERVAL_MINUTES,
            TimeUnit.MINUTES));
      } catch (final Exception e) {
        log.warn("Failed to initialize websocket", e);
        context.getClient().close(1011, "Unexpected error initializing connection");
      }
    } else {
      openWebsocketAtomicInteger.incrementAndGet();
      final Timer.Sample sample = Timer.start();
      context.addWebsocketClosedListener((context1, statusCode, reason) -> {
        openWebsocketAtomicInteger.decrementAndGet();
        sample.stop(connectionTimer);
      });
    }
  }

  private AtomicInteger getOpenWebsocketCounter(final String userAgentString, final boolean authenticated) {
    try {
      final ClientPlatform platform = UserAgentUtil.parseUserAgentString(userAgentString).getPlatform();
      return authenticated
          ? openAuthenticatedWebsocketsByClientPlatform.get(platform)
          : openUnauthenticatedWebsocketsByClientPlatform.get(platform);
    } catch (final UnrecognizedUserAgentException e) {
      return authenticated
          ? openAuthenticatedWebsocketsFromUnknownPlatforms
          : openUnauthenticatedWebsocketsFromUnknownPlatforms;
    }
  }

  private Timer getConnectionTimer(final String userAgentString,
      final boolean authenticated) {
    try {
      final ClientPlatform platform = UserAgentUtil.parseUserAgentString(userAgentString).getPlatform();
      return authenticated
          ? durationTimersByClientPlatform.get(platform)
          : unauthenticatedDurationTimersByClientPlatform.get(platform);
    } catch (final UnrecognizedUserAgentException e) {
      return authenticated
          ? durationTimerForUnknownPlatforms
          : unauthenticatedDurationTimerForUnknownPlatforms;
    }
  }
}
