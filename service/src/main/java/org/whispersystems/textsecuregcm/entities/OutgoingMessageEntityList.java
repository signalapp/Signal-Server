/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class OutgoingMessageEntityList {

  @JsonProperty
  private List<OutgoingMessageEntity> messages;

  @JsonProperty
  private boolean more;

  public OutgoingMessageEntityList() {}

  public OutgoingMessageEntityList(List<OutgoingMessageEntity> messages, boolean more) {
    this.messages = messages;
    this.more     = more;
  }

  public List<OutgoingMessageEntity> getMessages() {
    return messages;
  }

  public boolean hasMore() {
    return more;
  }
}
