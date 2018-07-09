package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class PushConfiguration {

  @JsonProperty
  @Min(0)
  private int queueSize = 200;

  public int getQueueSize() {
    return queueSize;
  }
}
