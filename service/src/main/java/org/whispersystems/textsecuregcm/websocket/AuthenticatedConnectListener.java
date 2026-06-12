/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.asn.AsnInfoProvider;
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

  private static final Logger log = LoggerFactory.getLogger(AuthenticatedConnectListener.class);

  private final AccountsManager accountsManager;
  private final DisconnectionRequestManager disconnectionRequestManager;
  private final WebSocketConnectionBuilder webSocketConnectionBuilder;
  private final MessageMetrics messageMetrics;

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
      final Supplier<AsnInfoProvider> asnInfoProviderSupplier,
      final ClientReleaseManager clientReleaseManager,
      final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this(accountsManager,
        disconnectionRequestManager,
        asnInfoProviderSupplier,
        clientReleaseManager,
        messageMetrics,
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
            experimentEnrollmentManager)
    );
  }

  @VisibleForTesting AuthenticatedConnectListener(
      final AccountsManager accountsManager,
      final DisconnectionRequestManager disconnectionRequestManager,
      final Supplier<AsnInfoProvider> asnInfoProviderSupplier,
      final ClientReleaseManager clientReleaseManager,
      final MessageMetrics messageMetrics,
      final WebSocketConnectionBuilder webSocketConnectionBuilder) {

    this.accountsManager = accountsManager;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.webSocketConnectionBuilder = webSocketConnectionBuilder;
    this.messageMetrics = messageMetrics;

    this.openAuthenticatedWebSocketCounter = new OpenWebSocketCounter("rpc-authenticated", asnInfoProviderSupplier, clientReleaseManager);
    this.openUnauthenticatedWebSocketCounter = new OpenWebSocketCounter("rpc-unauthenticated", asnInfoProviderSupplier, clientReleaseManager);
  }

  @Override
  public void onWebSocketConnect(final WebSocketSessionContext context) {

    final boolean authenticated = (context.getAuthenticated() != null);

    (authenticated ? openAuthenticatedWebSocketCounter : openUnauthenticatedWebSocketCounter).countOpenWebSocket(context);
    if (!authenticated) {
      return;
    }

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

    final Account account = maybeAuthenticatedAccount.get();
    final Device device = maybeAuthenticatedDevice.get();

    final boolean disableMessages = context.getClient().shouldDisableMessages();

    final Optional<WebSocketConnection> maybeWebSocketConnection = disableMessages
            ? Optional.empty()
            : Optional.of(webSocketConnectionBuilder.buildWebSocketConnection(account, device, context.getClient()));

    final WebSocketDisconnectionRequestListener disconnectionListener =
        new WebSocketDisconnectionRequestListener(messageMetrics, context.getClient(), disableMessages);

    disconnectionRequestManager
        .addListener(account.getIdentifier(IdentityType.ACI), device.getId(), disconnectionListener);

    context.addWebsocketClosedListener((_, _, _) -> {
      disconnectionRequestManager
          .removeListener(account.getIdentifier(IdentityType.ACI), device.getId(), disconnectionListener);
      maybeWebSocketConnection.ifPresent(WebSocketConnection::stop);
    });

    try {
      maybeWebSocketConnection.ifPresent(WebSocketConnection::start);
    } catch (final Exception e) {
      log.warn("Failed to initialize websocket", e);
      context.getClient().close(1011, "Unexpected error initializing connection");
    }
  }

}
