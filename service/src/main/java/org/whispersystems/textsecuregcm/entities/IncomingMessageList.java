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

public record IncomingMessageList(@NotNull @Valid List<@NotNull IncomingMessage> messages,
                                  boolean online, boolean urgent, long timestamp) {

  @JsonCreator
  public IncomingMessageList(@JsonProperty("messages") @NotNull @Valid List<@NotNull IncomingMessage> messages,
      @JsonProperty("online") boolean online,
      @JsonProperty("urgent") Boolean urgent,
      @JsonProperty("timestamp") long timestamp) {

    this(messages, online, urgent == null || urgent, timestamp);
  }
}
