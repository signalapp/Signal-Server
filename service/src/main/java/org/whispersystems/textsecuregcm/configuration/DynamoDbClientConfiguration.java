/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public record DynamoDbClientConfiguration(@NotBlank String region,
                                          @NotNull Duration clientExecutionTimeout,
                                          @NotNull Duration clientRequestTimeout,
                                          @Positive int maxConnections) {

  public DynamoDbClientConfiguration {
    if (clientExecutionTimeout == null) {
      clientExecutionTimeout = Duration.ofSeconds(30);
    }

    if (clientRequestTimeout == null) {
      clientRequestTimeout = Duration.ofSeconds(10);
    }

    if (maxConnections == 0) {
      maxConnections = 50;
    }
  }
}
