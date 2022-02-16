/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IncomingMessage {

  @JsonProperty
  private int type;

  @JsonProperty
  private final String destination;

  @JsonProperty
  private long destinationDeviceId;

  @JsonProperty
  private int destinationRegistrationId;

  @JsonProperty
  private final String body;

  @JsonProperty
  private final String content;

  @JsonProperty
  private final String relay;

  @JsonProperty
  private long timestamp; // deprecated

  @JsonCreator
  public IncomingMessage(
      @JsonProperty("id") final Integer type,
      @JsonProperty("destination") final String destination,
      @JsonProperty("destinationDeviceId") final Long destinationDeviceId,
      @JsonProperty("destinationRegistrationId") final Integer destinationRegistrationId,
      @JsonProperty("body") final String body,
      @JsonProperty("content") final String content,
      @JsonProperty("relay") final String relay,
      @JsonProperty("timestamp") final Long timestamp) {
    if (type != null) {
      this.type = type;
    }
    this.destination = destination;

    if (destinationDeviceId != null) {
      this.destinationDeviceId = destinationDeviceId;
    }
    if (destinationRegistrationId != null) {
      this.destinationRegistrationId = destinationRegistrationId;
    }
    this.body = body;
    this.content = content;
    this.relay = relay;
    if (timestamp != null) {
      this.timestamp = timestamp;
    }
  }

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
