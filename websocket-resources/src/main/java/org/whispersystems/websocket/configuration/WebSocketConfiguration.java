/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.websocket.logging.WebsocketRequestLoggerFactory;

public class WebSocketConfiguration {

  @Valid
  @NotNull
  @JsonProperty
  private WebsocketRequestLoggerFactory requestLog = new WebsocketRequestLoggerFactory();

  @Min(512 * 1024)       // 512 KB
  @Max(10 * 1024 * 1024) // 10 MB
  @JsonProperty
  private int maxBinaryMessageSize = 512 * 1024;

  @Min(512 * 1024)       // 512 KB
  @Max(10 * 1024 * 1024) // 10 MB
  @JsonProperty
  private int maxTextMessageSize = 512 * 1024;

  public WebsocketRequestLoggerFactory getRequestLog() {
    return requestLog;
  }

  public int getMaxBinaryMessageSize() {
    return maxBinaryMessageSize;
  }

  public int getMaxTextMessageSize() {
    return maxTextMessageSize;
  }
}
