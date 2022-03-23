/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IncomingMessage {

  @JsonProperty
  private final int type;

  @JsonProperty
  private final String destination;

  @JsonProperty
  private final long destinationDeviceId;

  @JsonProperty
  private final int destinationRegistrationId;

  @JsonProperty
  private final String content;

  @JsonCreator
  public IncomingMessage(
      @JsonProperty("id") final int type,
      @JsonProperty("destination") final String destination,
      @JsonProperty("destinationDeviceId") final long destinationDeviceId,
      @JsonProperty("destinationRegistrationId") final int destinationRegistrationId,
      @JsonProperty("content") final String content) {
    this.type = type;
    this.destination = destination;
    this.destinationDeviceId = destinationDeviceId;
    this.destinationRegistrationId = destinationRegistrationId;
    this.content = content;
  }

  public String getDestination() {
    return destination;
  }

  public int getType() {
    return type;
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
