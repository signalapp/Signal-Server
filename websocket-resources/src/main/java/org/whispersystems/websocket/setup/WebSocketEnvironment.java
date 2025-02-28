/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.setup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jersey.DropwizardResourceConfig;
import jakarta.validation.Validator;
import java.security.Principal;
import java.time.Duration;
import org.glassfish.jersey.server.ResourceConfig;
import org.whispersystems.websocket.auth.AuthenticatedWebSocketUpgradeFilter;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import javax.annotation.Nullable;

public class WebSocketEnvironment<T extends Principal> {

  private final ResourceConfig jerseyConfig;
  private final ObjectMapper objectMapper;
  private final Validator validator;
  private final WebsocketRequestLog requestLog;
  private final Duration idleTimeout;

  private WebSocketAuthenticator<T> authenticator;
  private AuthenticatedWebSocketUpgradeFilter<T> authenticatedWebSocketUpgradeFilter;
  private WebSocketMessageFactory messageFactory;
  private WebSocketConnectListener connectListener;

  public WebSocketEnvironment(Environment environment, WebSocketConfiguration configuration) {
    this(environment, configuration, Duration.ofMillis(60000));
  }

  public WebSocketEnvironment(Environment environment, WebSocketConfiguration configuration, Duration idleTimeout) {
    this(environment, configuration.getRequestLog().build("websocket"), idleTimeout);
  }

  public WebSocketEnvironment(Environment environment, WebsocketRequestLog requestLog, Duration idleTimeout) {
    this.jerseyConfig = new DropwizardResourceConfig(environment.metrics());
    this.objectMapper = environment.getObjectMapper();
    this.validator = environment.getValidator();
    this.requestLog = requestLog;
    this.messageFactory = new ProtobufWebSocketMessageFactory();
    this.idleTimeout = idleTimeout;
  }

  public ResourceConfig jersey() {
    return jerseyConfig;
  }

  @Nullable
  public WebSocketAuthenticator<T> getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(WebSocketAuthenticator<T> authenticator) {
    this.authenticator = authenticator;
  }

  @Nullable
  public AuthenticatedWebSocketUpgradeFilter<T> getAuthenticatedWebSocketUpgradeFilter() {
    return authenticatedWebSocketUpgradeFilter;
  }

  public void setAuthenticatedWebSocketUpgradeFilter(final AuthenticatedWebSocketUpgradeFilter<T> authenticatedWebSocketUpgradeFilter) {
    this.authenticatedWebSocketUpgradeFilter = authenticatedWebSocketUpgradeFilter;
  }

  public Duration getIdleTimeout() {
    return idleTimeout;
  }

  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  public WebsocketRequestLog getRequestLog() {
    return requestLog;
  }

  public Validator getValidator() {
    return validator;
  }

  public WebSocketMessageFactory getMessageFactory() {
    return messageFactory;
  }

  public void setMessageFactory(WebSocketMessageFactory messageFactory) {
    this.messageFactory = messageFactory;
  }

  public WebSocketConnectListener getConnectListener() {
    return connectListener;
  }

  public void setConnectListener(WebSocketConnectListener connectListener) {
    this.connectListener = connectListener;
  }
}
