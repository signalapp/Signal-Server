/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public record DynamicNotificationConfiguration(@NotNull Duration undeliveredMessagesNotificationDelay) {

  public static final DynamicNotificationConfiguration DEFAULT =
      new DynamicNotificationConfiguration(Duration.ofMinutes(1));

  public DynamicNotificationConfiguration {
    if (undeliveredMessagesNotificationDelay == null) {
      undeliveredMessagesNotificationDelay = DEFAULT.undeliveredMessagesNotificationDelay();
    }
  }
}
