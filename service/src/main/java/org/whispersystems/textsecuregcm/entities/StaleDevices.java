/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public class StaleDevices {

  @JsonProperty
  @Schema(description = "Devices that are no longer active")
  private List<Long> staleDevices;

  public StaleDevices() {}

  public String toString() {
    return "StaleDevices(" + staleDevices + ")";
  }

  public StaleDevices(List<Long> staleDevices) {
    this.staleDevices = staleDevices;
  }

}
