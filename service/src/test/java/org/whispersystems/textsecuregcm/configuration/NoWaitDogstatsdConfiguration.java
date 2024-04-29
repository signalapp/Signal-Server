/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Duration;

@JsonTypeName("nowait")
public class NoWaitDogstatsdConfiguration extends DogstatsdConfiguration {

  @Override
  public Duration getShutdownWaitDuration() {
    return Duration.ZERO;
  }
}
