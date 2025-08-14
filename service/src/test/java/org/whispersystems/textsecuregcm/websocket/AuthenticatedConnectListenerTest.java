/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.OpenWebSocketCounter;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.session.WebSocketSessionContext;

class AuthenticatedConnectListenerTest {

  private AccountsManager accountsManager;
  private DisconnectionRequestManager disconnectionRequestManager;

  private WebSocketConnection authenticatedWebSocketConnection;
  private AuthenticatedConnectListener authenticatedConnectListener;

  private Account authenticatedAccount;
  private WebSocketClient webSocketClient;
  private WebSocketSessionContext webSocketSessionContext;

  private static final UUID ACCOUNT_IDENTIFIER = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  @BeforeEach
  void setUpBeforeEach() {
    accountsManager = mock(AccountsManager.class);
    disconnectionRequestManager = mock(DisconnectionRequestManager.class);

    authenticatedWebSocketConnection = mock(WebSocketConnection.class);

    authenticatedConnectListener = new AuthenticatedConnectListener(accountsManager,
        disconnectionRequestManager,
        (_, _, _) -> authenticatedWebSocketConnection,
        _ -> mock(OpenWebSocketCounter.class));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    authenticatedAccount = mock(Account.class);
    when(authenticatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTIFIER);
    when(authenticatedAccount.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));

    webSocketClient = mock(WebSocketClient.class);

    webSocketSessionContext = mock(WebSocketSessionContext.class);
    when(webSocketSessionContext.getClient()).thenReturn(webSocketClient);
  }

  @Test
  void onWebSocketConnectAuthenticated() {
    when(webSocketSessionContext.getAuthenticated()).thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));
    when(webSocketSessionContext.getAuthenticated(AuthenticatedDevice.class))
        .thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));

    when(accountsManager.getByAccountIdentifier(ACCOUNT_IDENTIFIER)).thenReturn(Optional.of(authenticatedAccount));

    authenticatedConnectListener.onWebSocketConnect(webSocketSessionContext);

    verify(disconnectionRequestManager).addListener(ACCOUNT_IDENTIFIER, DEVICE_ID, authenticatedWebSocketConnection);
    verify(webSocketSessionContext).addWebsocketClosedListener(any());
    verify(authenticatedWebSocketConnection).start();
  }

  @Test
  void onWebSocketConnectAuthenticatedAccountNotFound() {
    when(webSocketSessionContext.getAuthenticated()).thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));
    when(webSocketSessionContext.getAuthenticated(AuthenticatedDevice.class))
        .thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));

    when(accountsManager.getByAccountIdentifier(ACCOUNT_IDENTIFIER)).thenReturn(Optional.empty());

    authenticatedConnectListener.onWebSocketConnect(webSocketSessionContext);

    verify(webSocketClient).close(eq(1011), anyString());

    verify(disconnectionRequestManager, never()).addListener(any(), anyByte(), any());
    verify(webSocketSessionContext, never()).addWebsocketClosedListener(any());
    verify(authenticatedWebSocketConnection, never()).start();
  }

  @Test
  void onWebSocketConnectAuthenticatedStartException() {
    when(webSocketSessionContext.getAuthenticated()).thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));
    when(webSocketSessionContext.getAuthenticated(AuthenticatedDevice.class))
        .thenReturn(new AuthenticatedDevice(ACCOUNT_IDENTIFIER, DEVICE_ID, Instant.now()));

    when(accountsManager.getByAccountIdentifier(ACCOUNT_IDENTIFIER)).thenReturn(Optional.of(authenticatedAccount));
    doThrow(new RuntimeException()).when(authenticatedWebSocketConnection).start();

    authenticatedConnectListener.onWebSocketConnect(webSocketSessionContext);

    verify(disconnectionRequestManager).addListener(ACCOUNT_IDENTIFIER, DEVICE_ID, authenticatedWebSocketConnection);
    verify(webSocketSessionContext).addWebsocketClosedListener(any());
    verify(authenticatedWebSocketConnection).start();

    verify(webSocketClient).close(eq(1011), anyString());
  }

  @Test
  void onWebSocketConnectUnauthenticated() {
    authenticatedConnectListener.onWebSocketConnect(webSocketSessionContext);

    verify(disconnectionRequestManager, never()).addListener(any(), anyByte(), any());
    verify(webSocketSessionContext, never()).addWebsocketClosedListener(any());
    verify(authenticatedWebSocketConnection, never()).start();
  }
}
