/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicAbusiveHostRulesMigrationConfiguration {

  @JsonProperty
  private boolean newReadEnabled = false;

  @JsonProperty
  private boolean newWriteEnabled = false;

  @JsonProperty
  private boolean newPrimary = false;

  public boolean isNewReadEnabled() {
    return newReadEnabled;
  }

  public boolean isNewWriteEnabled() {
    return newWriteEnabled;
  }

  public boolean isNewPrimary() {
    return newPrimary;
  }
}
