package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class MissingDevices {

  @JsonProperty
  public List<Long> missingDevices;

  public MissingDevices(List<Long> missingDevices) {
    this.missingDevices = missingDevices;
  }

}
