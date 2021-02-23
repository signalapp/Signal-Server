/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Set;

public class DynamicMessageRateConfiguration {

  @JsonProperty
  private boolean enforceUnsealedSenderRateLimit = false;

  @JsonProperty
  private Set<String> rateLimitedCountryCodes = Collections.emptySet();

  public boolean isEnforceUnsealedSenderRateLimit() {
    return enforceUnsealedSenderRateLimit;
  }

  public Set<String> getRateLimitedCountryCodes() {
    return rateLimitedCountryCodes;
  }
}
