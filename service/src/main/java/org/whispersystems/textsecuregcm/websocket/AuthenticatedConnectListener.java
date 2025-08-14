/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.OpenWebSocketCounter;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;
import reactor.core.scheduler.Scheduler;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final String OPEN_WEBSOCKET_GAUGE_NAME = name(WebSocketConnection.class, "openWebsockets");
  private static final String NEW_CONNECTION_COUNTER_NAME = name(AuthenticatedConnectListener.class, "newConnections");
  private static final String CONNECTED_DURATION_TIMER_NAME =
      name(AuthenticatedConnectListener.class, "connectedDuration");

  private static final String AUTHENTICATED_TAG_NAME = "authenticated";

  private static final Logger log = LoggerFactory.getLogger(AuthenticatedConnectListener.class);

  private final AccountsManager accountsManager;
  private final DisconnectionRequestManager disconnectionRequestManager;
  private final WebSocketConnectionBuilder webSocketConnectionBuilder;

  private final OpenWebSocketCounter openAuthenticatedWebSocketCounter;
  private final OpenWebSocketCounter openUnauthenticatedWebSocketCounter;

  @VisibleForTesting
  @FunctionalInterface
  interface WebSocketConnectionBuilder {
    WebSocketConnection buildWebSocketConnection(Account account, Device device, WebSocketClient client);
  }

  public AuthenticatedConnectListener(
      final AccountsManager accountsManager,
      final ReceiptSender receiptSender,
      final MessagesManager messagesManager,
      final MessageMetrics messageMetrics,
      final PushNotificationManager pushNotificationManager,
      final PushNotificationScheduler pushNotificationScheduler,
      final DisconnectionRequestManager disconnectionRequestManager,
      final Scheduler messageDeliveryScheduler,
      final ClientReleaseManager clientReleaseManager,
      final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this(accountsManager,
        disconnectionRequestManager,
        (account, device, client) -> new WebSocketConnection(receiptSender,
            messagesManager,
            messageMetrics,
            pushNotificationManager,
            pushNotificationScheduler,
            account,
            device,
            client,
            messageDeliveryScheduler,
            clientReleaseManager,
            messageDeliveryLoopMonitor,
            experimentEnrollmentManager),
        authenticated -> new OpenWebSocketCounter(OPEN_WEBSOCKET_GAUGE_NAME,
            NEW_CONNECTION_COUNTER_NAME,
            CONNECTED_DURATION_TIMER_NAME,
            Tags.of(AUTHENTICATED_TAG_NAME, String.valueOf(authenticated)))
    );
  }

  @VisibleForTesting AuthenticatedConnectListener(
      final AccountsManager accountsManager,
      final DisconnectionRequestManager disconnectionRequestManager,
      final WebSocketConnectionBuilder webSocketConnectionBuilder,
      final Function<Boolean, OpenWebSocketCounter> openWebSocketCounterBuilder) {

    this.accountsManager = accountsManager;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.webSocketConnectionBuilder = webSocketConnectionBuilder;

    openAuthenticatedWebSocketCounter = openWebSocketCounterBuilder.apply(true);
    openUnauthenticatedWebSocketCounter = openWebSocketCounterBuilder.apply(false);
  }

  @Override
  public void onWebSocketConnect(final WebSocketSessionContext context) {

    final boolean authenticated = (context.getAuthenticated() != null);

    (authenticated ? openAuthenticatedWebSocketCounter : openUnauthenticatedWebSocketCounter).countOpenWebSocket(context);

    if (authenticated) {
      final AuthenticatedDevice auth = context.getAuthenticated(AuthenticatedDevice.class);

      final Optional<Account> maybeAuthenticatedAccount =
          accountsManager.getByAccountIdentifier(auth.accountIdentifier());

      final Optional<Device> maybeAuthenticatedDevice =
          maybeAuthenticatedAccount.flatMap(account -> account.getDevice(auth.deviceId()));

      if (maybeAuthenticatedAccount.isEmpty() || maybeAuthenticatedDevice.isEmpty()) {
        log.warn("{}:{} not found when opening authenticated WebSocket", auth.accountIdentifier(), auth.deviceId());

        context.getClient().close(1011, "Unexpected error initializing connection");
        return;
      }

      final WebSocketConnection connection =
          webSocketConnectionBuilder.buildWebSocketConnection(maybeAuthenticatedAccount.get(),
              maybeAuthenticatedDevice.get(),
              context.getClient());

      disconnectionRequestManager.addListener(maybeAuthenticatedAccount.get().getIdentifier(IdentityType.ACI),
          maybeAuthenticatedDevice.get().getId(),
          connection);

      context.addWebsocketClosedListener((_, _, _) -> {
        disconnectionRequestManager.removeListener(maybeAuthenticatedAccount.get().getIdentifier(IdentityType.ACI),
            maybeAuthenticatedDevice.get().getId(),
            connection);

        connection.stop();
      });

      try {
        connection.start();
      } catch (final Exception e) {
        log.warn("Failed to initialize websocket", e);
        context.getClient().close(1011, "Unexpected error initializing connection");
      }
    }
  }
}
