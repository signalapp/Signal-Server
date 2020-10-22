/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.websocket.logging.WebsocketRequestLoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class WebSocketConfiguration {

  @Valid
  @NotNull
  @JsonProperty
  private WebsocketRequestLoggerFactory requestLog = new WebsocketRequestLoggerFactory();

  public WebsocketRequestLoggerFactory getRequestLog() {
    return requestLog;
  }
}
