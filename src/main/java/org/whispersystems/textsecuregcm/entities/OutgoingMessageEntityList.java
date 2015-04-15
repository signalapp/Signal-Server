package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class OutgoingMessageEntityList {

  @JsonProperty
  private List<OutgoingMessageEntity> messages;

  public OutgoingMessageEntityList() {}

  public OutgoingMessageEntityList(List<OutgoingMessageEntity> messages) {
    this.messages = messages;
  }

  @VisibleForTesting
  public List<OutgoingMessageEntity> getMessages() {
    return messages;
  }
}
