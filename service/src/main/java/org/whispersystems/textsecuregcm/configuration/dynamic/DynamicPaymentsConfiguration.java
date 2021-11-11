/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

public class DynamicPaymentsConfiguration {

  @JsonProperty
  private List<String> disallowedPrefixes = Collections.emptyList();

  public List<String> getDisallowedPrefixes() {
    return disallowedPrefixes;
  }
}
