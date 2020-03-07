/*
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.websocket;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.glassfish.jersey.server.ApplicationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.auth.WebSocketAuthenticator.AuthenticationResult;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Optional;

import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import static java.util.Optional.ofNullable;

public class WebSocketResourceProviderFactory<T extends Principal> extends WebSocketServlet implements WebSocketCreator {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketResourceProviderFactory.class);

  private final WebSocketEnvironment<T> environment;
  private final ApplicationHandler      jerseyApplicationHandler;

  public WebSocketResourceProviderFactory(WebSocketEnvironment<T> environment, Class<T> principalClass) {
    this.environment = environment;

    environment.jersey().register(new WebSocketSessionContextValueFactoryProvider.Binder());
    environment.jersey().register(new WebsocketAuthValueFactoryProvider.Binder<T>(principalClass));
    environment.jersey().register(new JacksonMessageBodyProvider(environment.getObjectMapper()));

    this.jerseyApplicationHandler = new ApplicationHandler(environment.jersey());
  }

  @Override
  public Object createWebSocket(ServletUpgradeRequest request, ServletUpgradeResponse response) {
    try {
      Optional<WebSocketAuthenticator<T>> authenticator = Optional.ofNullable(environment.getAuthenticator());
      T                                   authenticated = null;

      if (authenticator.isPresent()) {
        AuthenticationResult<T> authenticationResult = authenticator.get().authenticate(request);

        if (authenticationResult.getUser().isEmpty() && authenticationResult.isRequired()) {
          response.sendForbidden("Unauthorized");
          return null;
        } else {
          authenticated = authenticationResult.getUser().orElse(null);
        }
      }

      return new WebSocketResourceProvider<T>(getRemoteAddress(request),
                                              this.jerseyApplicationHandler,
                                              this.environment.getRequestLog(),
                                              authenticated,
                                              this.environment.getMessageFactory(),
                                              ofNullable(this.environment.getConnectListener()),
                                              this.environment.getIdleTimeoutMillis());
    } catch (AuthenticationException | IOException e) {
      logger.warn("Authentication failure", e);
      try {
        response.sendError(500, "Failure");
      } catch (IOException ex) {}
      return null;
    }
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.setCreator(this);
  }

  private String getRemoteAddress(ServletUpgradeRequest request) {
    String forwardedFor = request.getHeader("X-Forwarded-For");

    if (forwardedFor == null || forwardedFor.isBlank()) {
      return request.getRemoteAddress();
    } else {
      return Arrays.stream(forwardedFor.split(","))
                   .map(String::trim)
                   .reduce((a, b) -> b)
                   .orElseThrow();
    }
  }
}
