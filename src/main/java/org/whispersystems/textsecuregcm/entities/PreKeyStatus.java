package org.whispersystems.textsecuregcm.entities;


import com.fasterxml.jackson.annotation.JsonProperty;

public class PreKeyStatus {

  @JsonProperty
  private int count;

  public PreKeyStatus(int count) {
    this.count = count;
  }

  public PreKeyStatus() {}

  public int getCount() {
    return count;
  }
}
