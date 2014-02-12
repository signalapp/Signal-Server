package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class MismatchedDevices {

  @JsonProperty
  public List<Long> missingDevices;

  @JsonProperty
  public List<Long> extraDevices;

  @VisibleForTesting
  public MismatchedDevices() {}

  public MismatchedDevices(List<Long> missingDevices, List<Long> extraDevices) {
    this.missingDevices = missingDevices;
    this.extraDevices   = extraDevices;
  }

}
