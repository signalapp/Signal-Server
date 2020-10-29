/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IncomingMessage {

  @JsonProperty
  private int    type;

  @JsonProperty
  private String destination;

  @JsonProperty
  private long   destinationDeviceId = 1;

  @JsonProperty
  private int destinationRegistrationId;

  @JsonProperty
  private String body;

  @JsonProperty
  private String content;

  @JsonProperty
  private String relay;

  @JsonProperty
  private long   timestamp; // deprecated

  public String getDestination() {
    return destination;
  }

  public String getBody() {
    return body;
  }

  public int getType() {
    return type;
  }

  public String getRelay() {
    return relay;
  }

  public long getDestinationDeviceId() {
    return destinationDeviceId;
  }

  public int getDestinationRegistrationId() {
    return destinationRegistrationId;
  }

  public String getContent() {
    return content;
  }

}
