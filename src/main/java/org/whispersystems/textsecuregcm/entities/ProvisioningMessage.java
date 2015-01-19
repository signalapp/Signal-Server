package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProvisioningMessage {

  @JsonProperty
  private String body;

  public ProvisioningMessage() {}

  public ProvisioningMessage(String body) {
    this.body = body;
  }

  public String getBody() {
    return body;
  }
}
