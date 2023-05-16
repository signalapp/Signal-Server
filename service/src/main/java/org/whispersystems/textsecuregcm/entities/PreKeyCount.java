/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public class PreKeyCount {

  @Schema(description="the number of stored unsigned elliptic-curve prekeys for this device")
  @JsonProperty
  private int count;

  @Schema(description="the number of stored one-time post-quantum prekeys for this device")
  @JsonProperty
  private int pqCount;

  public PreKeyCount(int ecCount, int pqCount) {
    this.count = ecCount;
    this.pqCount = pqCount;
  }

  public PreKeyCount() {}

  public int getCount() {
    return count;
  }

  public int getPqCount() {
    return pqCount;
  }
}
