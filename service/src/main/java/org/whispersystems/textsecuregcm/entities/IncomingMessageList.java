/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class IncomingMessageList {

  @JsonProperty
  @NotNull
  @Valid
  private final List<@NotNull IncomingMessage> messages;

  @JsonProperty
  private long timestamp;

  @JsonProperty
  private boolean online;

  @JsonCreator
  public IncomingMessageList(
      @JsonProperty("messages") final List<@NotNull IncomingMessage> messages,
      @JsonProperty("online") final Boolean online,
      @JsonProperty("timestamp") final Long timestamp) {
    this.messages = messages;
    if (timestamp != null) {
      this.timestamp = timestamp;
    }
    if (online != null) {
      this.online = online;
    }
  }

  public List<IncomingMessage> getMessages() {
    return messages;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isOnline() {
    return online;
  }
}
