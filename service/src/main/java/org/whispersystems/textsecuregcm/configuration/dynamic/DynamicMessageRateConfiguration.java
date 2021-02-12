/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicMessageRateConfiguration {

  @JsonProperty
  private boolean enforceUnsealedSenderRateLimit = false;

  public boolean isEnforceUnsealedSenderRateLimit() {
    return enforceUnsealedSenderRateLimit;
  }
}
