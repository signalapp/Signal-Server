/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public record TotpConfiguration(@NotNull Duration maxValidationDelay) {

  public static TotpConfiguration DEFAULT = new TotpConfiguration(Duration.ofSeconds(15));

  public TotpConfiguration {
    if (maxValidationDelay == null) {
      maxValidationDelay = DEFAULT.maxValidationDelay();
    }
  }
}
