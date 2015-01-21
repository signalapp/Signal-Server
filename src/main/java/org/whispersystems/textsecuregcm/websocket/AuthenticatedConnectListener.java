package org.whispersystems.textsecuregcm.websocket;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

  private final AccountsManager accountsManager;
  private final PushSender      pushSender;
  private final StoredMessages  storedMessages;
  private final PubSubManager   pubSubManager;

  public AuthenticatedConnectListener(AccountsManager accountsManager, PushSender pushSender,
                                      StoredMessages storedMessages, PubSubManager pubSubManager)
  {
    this.accountsManager = accountsManager;
    this.pushSender      = pushSender;
    this.storedMessages  = storedMessages;
    this.pubSubManager   = pubSubManager;
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    Optional<Account> account = context.getAuthenticated(Account.class);

    if (!account.isPresent()) {
      logger.debug("WS Connection with no authentication...");
      context.getClient().close(4001, "Authentication failed");
      return;
    }

    Optional<Device> device = account.get().getAuthenticatedDevice();

    if (!device.isPresent()) {
      logger.debug("WS Connection with no authenticated device...");
      context.getClient().close(4001, "Device authentication failed");
      return;
    }

    if (device.get().getLastSeen() != Util.todayInMillis()) {
      device.get().setLastSeen(Util.todayInMillis());
      accountsManager.update(account.get());
    }

    final WebSocketConnection connection = new WebSocketConnection(accountsManager, pushSender,
                                                                   storedMessages, pubSubManager,
                                                                   account.get(), device.get(),
                                                                   context.getClient());

    connection.onConnected();

    context.addListener(new WebSocketSessionContext.WebSocketEventListener() {
      @Override
      public void onWebSocketClose(WebSocketSessionContext context, int statusCode, String reason) {
        connection.onConnectionLost();
      }
    });
  }
}
