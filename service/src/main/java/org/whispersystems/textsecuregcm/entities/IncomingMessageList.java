/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class IncomingMessageList {

  @JsonProperty
  @NotNull
  @Valid
  private List<IncomingMessage> messages;

  @JsonProperty
  private long timestamp;

  @JsonProperty
  private boolean online;

  public IncomingMessageList() {}

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
