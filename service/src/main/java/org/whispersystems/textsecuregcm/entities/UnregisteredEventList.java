package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;

public class UnregisteredEventList {

  @JsonProperty
  private List<UnregisteredEvent> devices;

  public List<UnregisteredEvent> getDevices() {
    if (devices == null) return new LinkedList<>();
    else                 return devices;
  }
}
