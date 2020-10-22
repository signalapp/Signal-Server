/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.setup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.ResourceConfig;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;

import javax.validation.Validator;
import java.security.Principal;

public class WebSocketEnvironment<T extends Principal> {

  private final ResourceConfig        jerseyConfig;
  private final ObjectMapper          objectMapper;
  private final Validator             validator;
  private final WebsocketRequestLog   requestLog;
  private final long                  idleTimeoutMillis;

  private WebSocketAuthenticator<T> authenticator;
  private WebSocketMessageFactory   messageFactory;
  private WebSocketConnectListener  connectListener;

  public WebSocketEnvironment(Environment environment, WebSocketConfiguration configuration) {
    this(environment, configuration, 60000);
  }

  public WebSocketEnvironment(Environment environment, WebSocketConfiguration configuration, long idleTimeoutMillis) {
    this(environment, configuration.getRequestLog().build("websocket"), idleTimeoutMillis);
  }

  public WebSocketEnvironment(Environment environment, WebsocketRequestLog requestLog, long idleTimeoutMillis) {
    this.jerseyConfig             = new DropwizardResourceConfig(environment.metrics());
    this.objectMapper             = environment.getObjectMapper();
    this.validator                = environment.getValidator();
    this.requestLog               = requestLog;
    this.messageFactory           = new ProtobufWebSocketMessageFactory();
    this.idleTimeoutMillis        = idleTimeoutMillis;
  }

  public ResourceConfig jersey() {
    return jerseyConfig;
  }

  public WebSocketAuthenticator<T> getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(WebSocketAuthenticator<T> authenticator) {
    this.authenticator = authenticator;
  }

  public long getIdleTimeoutMillis() {
    return idleTimeoutMillis;
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
