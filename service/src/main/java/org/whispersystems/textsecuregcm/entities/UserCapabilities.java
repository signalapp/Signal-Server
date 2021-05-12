/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCapabilities {
  @JsonProperty
  private boolean gv2;

  @JsonProperty("gv1-migration")
  private boolean gv1Migration;

  @JsonProperty
  private boolean senderKey;

  public UserCapabilities() {}

  public UserCapabilities(boolean gv2, boolean gv1Migration, final boolean senderKey) {
    this.gv2  = gv2;
    this.gv1Migration = gv1Migration;
    this.senderKey = senderKey;
  }

  public boolean isGv2() {
    return gv2;
  }

  public boolean isGv1Migration() {
    return gv1Migration;
  }

  public boolean isSenderKey() {
    return senderKey;
  }
}
