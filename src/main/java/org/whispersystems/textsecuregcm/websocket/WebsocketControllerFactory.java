package org.whispersystems.textsecuregcm.websocket;

import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.controllers.WebsocketController;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.WebsocketSender;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessages;


public class WebsocketControllerFactory extends WebSocketServlet implements WebSocketCreator {

  private final Logger logger = LoggerFactory.getLogger(WebsocketControllerFactory.class);

  private final PushSender           pushSender;
  private final StoredMessages       storedMessages;
  private final PubSubManager        pubSubManager;
  private final AccountAuthenticator accountAuthenticator;

  public WebsocketControllerFactory(AccountAuthenticator accountAuthenticator,
                                    PushSender           pushSender,
                                    StoredMessages       storedMessages,
                                    PubSubManager        pubSubManager)
  {
    this.accountAuthenticator = accountAuthenticator;
    this.pushSender           = pushSender;
    this.storedMessages       = storedMessages;
    this.pubSubManager        = pubSubManager;
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.setCreator(this);
  }

  @Override
  public Object createWebSocket(UpgradeRequest upgradeRequest, UpgradeResponse upgradeResponse) {
    return new WebsocketController(accountAuthenticator, pushSender, pubSubManager, storedMessages);
  }
}
