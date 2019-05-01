package org.whispersystems.websocket.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import io.dropwizard.request.logging.LogbackAccessRequestLogFactory;
import io.dropwizard.request.logging.RequestLogFactory;

public class WebSocketConfiguration {

  @Valid
  @NotNull
  @JsonProperty
  private RequestLogFactory requestLog = new LogbackAccessRequestLogFactory();

  public RequestLogFactory getRequestLog() {
    return requestLog;
  }
}
