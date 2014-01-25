package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IncomingWebsocketMessage {

  public static final int TYPE_ACKNOWLEDGE_MESSAGE = 1;
  public static final int TYPE_PING_MESSAGE        = 2;
  public static final int TYPE_PONG_MESSAGE        = 3;

  @JsonProperty
  protected int type;

  public IncomingWebsocketMessage() {}

  public IncomingWebsocketMessage(int type) {
    this.type = type;
  }

  public int getType() {
    return type;
  }
}
