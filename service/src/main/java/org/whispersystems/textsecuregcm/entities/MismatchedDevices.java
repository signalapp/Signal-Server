/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public class MismatchedDevices {

  @JsonProperty
  @Schema(description = "Devices present on the account but absent in the request")
  public List<Long> missingDevices;

  @JsonProperty
  @Schema(description = "Devices absent on the request but present in the account")
  public List<Long> extraDevices;

  @VisibleForTesting
  public MismatchedDevices() {}

  public String toString() {
    return "MismatchedDevices(" + missingDevices + ", " + extraDevices + ")";
  }

  public MismatchedDevices(List<Long> missingDevices, List<Long> extraDevices) {
    this.missingDevices = missingDevices;
    this.extraDevices   = extraDevices;
  }

}
