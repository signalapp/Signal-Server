/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

public record DynamicPaymentsConfiguration(@JsonProperty @NotNull List<String> disallowedPrefixes,
                                           @JsonProperty @NotNull List<String> disallowedAsnRegions) {

  public static DynamicPaymentsConfiguration DEFAULT =
      new DynamicPaymentsConfiguration(Collections.emptyList(), Collections.emptyList());

  public DynamicPaymentsConfiguration {
    if (disallowedPrefixes == null) {
      disallowedPrefixes = DEFAULT.disallowedPrefixes();
    }

    if (disallowedAsnRegions == null) {
      disallowedAsnRegions = DEFAULT.disallowedAsnRegions();
    }
  }
}
