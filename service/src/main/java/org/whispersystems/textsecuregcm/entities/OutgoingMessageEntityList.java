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
