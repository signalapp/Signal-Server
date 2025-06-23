/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.OpenWebSocketCounter;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;
import reactor.core.scheduler.Scheduler;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final String OPEN_WEBSOCKET_GAUGE_NAME = name(WebSocketConnection.class, "openWebsockets");
  private static final String CONNECTED_DURATION_TIMER_NAME =
      name(AuthenticatedConnectListener.class, "connectedDuration");

  private static final String AUTHENTICATED_TAG_NAME = "authenticated";

  private static final Logger log = LoggerFactory.getLogger(AuthenticatedConnectListener.class);

  private final AccountsManager accountsManager;
  private final ReceiptSender receiptSender;
  private final MessagesManager messagesManager;
  private final MessageMetrics messageMetrics;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final WebSocketConnectionEventManager webSocketConnectionEventManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Scheduler messageDeliveryScheduler;
  private final ClientReleaseManager clientReleaseManager;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final OpenWebSocketCounter openAuthenticatedWebSocketCounter;
  private final OpenWebSocketCounter openUnauthenticatedWebSocketCounter;

  public AuthenticatedConnectListener(
      AccountsManager accountsManager,
      ReceiptSender receiptSender,
      MessagesManager messagesManager,
      MessageMetrics messageMetrics,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      WebSocketConnectionEventManager webSocketConnectionEventManager,
      ScheduledExecutorService scheduledExecutorService,
      Scheduler messageDeliveryScheduler,
      ClientReleaseManager clientReleaseManager,
      MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this.accountsManager = accountsManager;
    this.receiptSender = receiptSender;
    this.messagesManager = messagesManager;
    this.messageMetrics = messageMetrics;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.webSocketConnectionEventManager = webSocketConnectionEventManager;
    this.scheduledExecutorService = scheduledExecutorService;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    openAuthenticatedWebSocketCounter =
        new OpenWebSocketCounter(OPEN_WEBSOCKET_GAUGE_NAME, CONNECTED_DURATION_TIMER_NAME, Tags.of(AUTHENTICATED_TAG_NAME, "true"));

    openUnauthenticatedWebSocketCounter =
        new OpenWebSocketCounter(OPEN_WEBSOCKET_GAUGE_NAME, CONNECTED_DURATION_TIMER_NAME, Tags.of(AUTHENTICATED_TAG_NAME, "false"));
  }

  @Override
  public void onWebSocketConnect(final WebSocketSessionContext context) {

    final boolean authenticated = (context.getAuthenticated() != null);
    final OpenWebSocketCounter openWebSocketCounter =
        authenticated ? openAuthenticatedWebSocketCounter : openUnauthenticatedWebSocketCounter;

    openWebSocketCounter.countOpenWebSocket(context);

    if (authenticated) {
      final AuthenticatedDevice auth = context.getAuthenticated(AuthenticatedDevice.class);

      final Optional<Account> maybeAuthenticatedAccount = accountsManager.getByAccountIdentifier(auth.accountIdentifier());
      final Optional<Device> maybeAuthenticatedDevice = maybeAuthenticatedAccount.flatMap(account -> account.getDevice(auth.deviceId()));;

      if (maybeAuthenticatedAccount.isEmpty() || maybeAuthenticatedDevice.isEmpty()) {
        log.warn("{}:{} not found when opening authenticated WebSocket", auth.accountIdentifier(), auth.deviceId());

        context.getClient().close(1011, "Unexpected error initializing connection");
        return;
      }

      final WebSocketConnection connection = new WebSocketConnection(receiptSender,
          messagesManager,
          messageMetrics,
          pushNotificationManager,
          pushNotificationScheduler,
          maybeAuthenticatedAccount.get(),
          maybeAuthenticatedDevice.get(),
          context.getClient(),
          scheduledExecutorService,
          messageDeliveryScheduler,
          clientReleaseManager,
          messageDeliveryLoopMonitor,
          experimentEnrollmentManager);

      context.addWebsocketClosedListener((closingContext, statusCode, reason) -> {
        // We begin the shutdown process by removing this client's "presence," which means it will again begin to
        // receive push notifications for inbound messages. We should do this first because, at this point, the
        // connection has already closed and attempts to actually deliver a message via the connection will not succeed.
        // It's preferable to start sending push notifications as soon as possible.
        webSocketConnectionEventManager.handleClientDisconnected(auth.accountIdentifier(), auth.deviceId());

        // Finally, stop trying to deliver messages and send a push notification if the connection is aware of any
        // undelivered messages.
        connection.stop();
      });

      try {
        // Once we "start" the websocket connection, we'll cancel any scheduled "you may have new messages" push
        // notifications and begin delivering any stored messages for the connected device. We have not yet declared the
        // client as "present" yet. If a message arrives at this point, we will update the message availability state
        // correctly, but we may also send a spurious push notification.
        connection.start();

        // Finally, we register this client's presence, which suppresses push notifications. We do this last because
        // receiving extra push notifications is generally preferable to missing out on a push notification.
        webSocketConnectionEventManager.handleClientConnected(auth.accountIdentifier(), auth.deviceId(), connection);
      } catch (final Exception e) {
        log.warn("Failed to initialize websocket", e);
        context.getClient().close(1011, "Unexpected error initializing connection");
      }
    }
  }
}
