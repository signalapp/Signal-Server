/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import static java.util.Optional.ofNullable;

import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import jakarta.ws.rs.InternalServerErrorException;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.server.ApplicationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.websocket.auth.InvalidCredentialsException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
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

  private final String remoteAddressPropertyName;

  public WebSocketResourceProviderFactory(WebSocketEnvironment<T> environment, Class<T> principalClass,
      WebSocketConfiguration configuration, String remoteAddressPropertyName) {
    this.environment = environment;

    environment.jersey().register(new WebSocketSessionContextValueFactoryProvider.Binder());
    environment.jersey().register(new WebsocketAuthValueFactoryProvider.Binder<>(principalClass));
    environment.jersey().register(new JacksonMessageBodyProvider(environment.getObjectMapper()));

    // Jersey buffers responses (by default up to 8192 bytes) just so it can add a content length to responses. We
    // already buffer our responses to serialize them as protos, so we can compute the content length ourselves. Setting
    // the buffer to zero disables buffering.
    environment.jersey().addProperties(Map.of(CommonProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0));

    this.jerseyApplicationHandler = new ApplicationHandler(environment.jersey());

    this.configuration = configuration;
    this.remoteAddressPropertyName = remoteAddressPropertyName;
  }

  @Override
  public Object createWebSocket(final JettyServerUpgradeRequest request, final JettyServerUpgradeResponse response) {
    try {
      Optional<WebSocketAuthenticator<T>> authenticator = Optional.ofNullable(environment.getAuthenticator());

      final Optional<T> authenticated = authenticator.isPresent()
          ? authenticator.get().authenticate(request)
          : Optional.empty();

      Optional.ofNullable(environment.getAuthenticatedWebSocketUpgradeFilter())
          .ifPresent(filter -> filter.handleAuthentication(authenticated, request, response));

      return new WebSocketResourceProvider<>(getRemoteAddress(request),
          remoteAddressPropertyName,
          this.jerseyApplicationHandler,
          this.environment.getRequestLog(),
          authenticated,
          this.environment.getMessageFactory(),
          ofNullable(this.environment.getConnectListener()),
          this.environment.getIdleTimeout());
    } catch (final InvalidCredentialsException e) {
      try {
        response.sendForbidden("Unauthorized");
      } catch (final IOException ignored) {
      }
      return null;
    } catch (final Exception e) {
      // Authentication may fail for non-incorrect-credential reasons (e.g. we couldn't read from the account database).
      // If that happens, we don't want to incorrectly tell clients that they provided bad credentials.
      logger.warn("Authentication failure", e);
      try {
        response.sendError(500, "Failure");
      } catch (final IOException ignored) {
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
    final String remoteAddress = (String) request.getHttpServletRequest().getAttribute(remoteAddressPropertyName);
    if (StringUtils.isBlank(remoteAddress)) {
      logger.error("Remote address property is not present");
      throw new InternalServerErrorException();
    }
    return remoteAddress;
  }
}
