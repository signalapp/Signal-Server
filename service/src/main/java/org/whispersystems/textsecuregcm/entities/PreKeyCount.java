/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;


import com.fasterxml.jackson.annotation.JsonProperty;

public class PreKeyCount {

  @JsonProperty
  private int count;

  public PreKeyCount(int count) {
    this.count = count;
  }

  public PreKeyCount() {}

  public int getCount() {
    return count;
  }
}
