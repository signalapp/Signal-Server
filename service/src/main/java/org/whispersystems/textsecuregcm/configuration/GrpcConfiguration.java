/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotNull;

public record GrpcConfiguration(@NotNull String bindAddress, @NotNull Integer port) {
  public GrpcConfiguration {
    if (bindAddress == null || bindAddress.isEmpty()) {
      bindAddress = "localhost";
    }
  }
}
