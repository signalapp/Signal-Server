package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StaleDevices {

  @JsonProperty
  private List<Long> staleDevices;

  public StaleDevices() {}

  public StaleDevices(List<Long> staleDevices) {
    this.staleDevices = staleDevices;
  }

}
