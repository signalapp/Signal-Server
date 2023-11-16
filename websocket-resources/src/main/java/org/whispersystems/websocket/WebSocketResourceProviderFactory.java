/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import static java.util.Optional.ofNullable;

import com.google.common.net.HttpHeaders;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.Arrays;
import java.util.Optional;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.glassfish.jersey.server.ApplicationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.auth.WebSocketAuthenticator.AuthenticationResult;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

public class WebSocketResourceProviderFactory<T extends Principal> extends JettyWebSocketServlet implements
    JettyWebSocketCreator {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketResourceProviderFactory.class);

  private final WebSocketEnvironment<T> environment;
  private final ApplicationHandler jerseyApplicationHandler;
  private final WebSocketConfiguration configuration;

  public WebSocketResourceProviderFactory(WebSocketEnvironment<T> environment, Class<T> principalClass,
      WebSocketConfiguration configuration) {
    this.environment = environment;

    environment.jersey().register(new WebSocketSessionContextValueFactoryProvider.Binder());
    environment.jersey().register(new WebsocketAuthValueFactoryProvider.Binder<T>(principalClass));
    environment.jersey().register(new JacksonMessageBodyProvider(environment.getObjectMapper()));

    this.jerseyApplicationHandler = new ApplicationHandler(environment.jersey());

    this.configuration = configuration;
  }

  @Override
  public Object createWebSocket(final JettyServerUpgradeRequest request, final JettyServerUpgradeResponse response) {
    try {
      Optional<WebSocketAuthenticator<T>> authenticator = Optional.ofNullable(environment.getAuthenticator());
      T authenticated = null;

      if (authenticator.isPresent()) {
        AuthenticationResult<T> authenticationResult = authenticator.get().authenticate(request);

        if (authenticationResult.getUser().isEmpty() && authenticationResult.credentialsPresented()) {
          response.sendForbidden("Unauthorized");
          return null;
        } else {
          authenticated = authenticationResult.getUser().orElse(null);
        }
      }

      return new WebSocketResourceProvider<>(getRemoteAddress(request),
          this.jerseyApplicationHandler,
          this.environment.getRequestLog(),
          authenticated,
          this.environment.getMessageFactory(),
          ofNullable(this.environment.getConnectListener()),
          this.environment.getIdleTimeout());
    } catch (AuthenticationException | IOException e) {
      logger.warn("Authentication failure", e);
      try {
        response.sendError(500, "Failure");
      } catch (IOException ignored) {
      }
      return null;
    }
  }

  @Override
  public void configure(JettyWebSocketServletFactory factory) {
    factory.setCreator(this);
    factory.setMaxBinaryMessageSize(configuration.getMaxBinaryMessageSize());
    factory.setMaxTextMessageSize(configuration.getMaxTextMessageSize());
  }

  private String getRemoteAddress(JettyServerUpgradeRequest request) {
    String forwardedFor = request.getHeader(HttpHeaders.X_FORWARDED_FOR);

    if (forwardedFor == null || forwardedFor.isBlank()) {
      if (request.getRemoteSocketAddress() instanceof InetSocketAddress inetSocketAddress) {
        return inetSocketAddress.getAddress().getHostAddress();
      }
      return null;
    } else {
      return Arrays.stream(forwardedFor.split(","))
          .map(String::trim)
                   .reduce((a, b) -> b)
                   .orElseThrow();
    }
  }
}
