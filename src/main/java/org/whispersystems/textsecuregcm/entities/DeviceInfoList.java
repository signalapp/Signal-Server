package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DeviceInfoList {

  @JsonProperty
  private List<DeviceInfo> devices;

  public DeviceInfoList(List<DeviceInfo> devices) {
    this.devices = devices;
  }
}
