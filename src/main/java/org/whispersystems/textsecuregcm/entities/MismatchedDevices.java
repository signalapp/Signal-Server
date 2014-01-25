package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class MismatchedDevices {

  @JsonProperty
  public List<Long> missingDevices;

  @JsonProperty
  public List<Long> extraDevices;

  public MismatchedDevices(List<Long> missingDevices, List<Long> extraDevices) {
    this.missingDevices = missingDevices;
    this.extraDevices   = extraDevices;
  }

}
