/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicMessageDeliveryConfiguration {

  @JsonProperty
  private boolean readOnly = false;

  public boolean isReadOnly() {
    return readOnly;
  }
}
