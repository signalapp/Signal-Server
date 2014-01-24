package org.whispersystems.textsecuregcm.websocket;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WebsocketMessage {

  @JsonProperty
  private long id;

  @JsonProperty
  private String message;

  public WebsocketMessage(long id, String message) {
    this.id      = id;
    this.message = message;
  }

}
