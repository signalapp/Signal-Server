package org.whispersystems.textsecuregcm.websocket;

import com.google.common.base.Optional;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;

import java.util.Map;

import io.dropwizard.auth.basic.BasicCredentials;


public class WebSocketAccountAuthenticator implements WebSocketAuthenticator<Account> {

  private final AccountAuthenticator accountAuthenticator;

  public WebSocketAccountAuthenticator(AccountAuthenticator accountAuthenticator) {
    this.accountAuthenticator = accountAuthenticator;
  }

  @Override
  public Optional<Account> authenticate(UpgradeRequest request) throws AuthenticationException {
    try {
      Map<String, String[]> parameters = request.getParameterMap();
      String[]              usernames  = parameters.get("login");
      String[]              passwords  = parameters.get("password");

      if (usernames == null || usernames.length == 0 ||
          passwords == null || passwords.length == 0)
      {
        return Optional.absent();
      }

      BasicCredentials credentials = new BasicCredentials(usernames[0], passwords[0]);
      return accountAuthenticator.authenticate(credentials);
    } catch (io.dropwizard.auth.AuthenticationException e) {
      throw new AuthenticationException(e);
    }
  }

}
