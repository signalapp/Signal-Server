package org.whispersystems.textsecuregcm.controllers;

import com.google.common.base.Optional;
import com.yammer.dropwizard.auth.AuthenticationException;
import com.yammer.dropwizard.auth.basic.BasicCredentials;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessageManager;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;


public class WebsocketControllerFactory extends WebSocketServlet {

  private final Logger logger = LoggerFactory.getLogger(WebsocketControllerFactory.class);

  private final StoredMessageManager storedMessageManager;
  private final PubSubManager        pubSubManager;
  private final AccountAuthenticator accountAuthenticator;

  private final LinkedHashMap<BasicCredentials, Optional<Account>> cache =
      new LinkedHashMap<BasicCredentials, Optional<Account>>() {
    @Override
    protected boolean removeEldestEntry(Map.Entry<BasicCredentials, Optional<Account>> eldest) {
      return size() > 10;
    }
  };

  public WebsocketControllerFactory(AccountAuthenticator accountAuthenticator,
                                    StoredMessageManager storedMessageManager,
                                    PubSubManager pubSubManager)
  {
    this.accountAuthenticator = accountAuthenticator;
    this.storedMessageManager = storedMessageManager;
    this.pubSubManager        = pubSubManager;
  }

  @Override
  public WebSocket doWebSocketConnect(HttpServletRequest request, String s) {
    try {
      String username = request.getParameter("user");
      String password = request.getParameter("password");

      if (username == null || password == null) {
        return null;
      }

      BasicCredentials credentials = new BasicCredentials(username, password);

      Optional<Account> account = cache.remove(credentials);

      if (account == null) {
        account = accountAuthenticator.authenticate(new BasicCredentials(username, password));
      }

      if (!account.isPresent()) {
        return null;
      }

      return new WebsocketController(storedMessageManager, pubSubManager, account.get());
    } catch (AuthenticationException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean checkOrigin(HttpServletRequest request, String origin) {
    try {
      String username = request.getParameter("user");
      String password = request.getParameter("password");

      if (username == null || password == null) {
        return false;
      }

      BasicCredentials  credentials = new BasicCredentials(username, password);
      Optional<Account> account     = accountAuthenticator.authenticate(credentials);

      if (!account.isPresent()) {
        return false;
      }

      cache.put(credentials, account);

      return true;
    } catch (AuthenticationException e) {
      logger.warn("Auth Failure", e);
      return false;
    }
  }
}
