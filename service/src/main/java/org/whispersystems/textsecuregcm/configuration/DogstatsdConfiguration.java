/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import java.time.Duration;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public class DogstatsdConfiguration implements StatsdConfig {

  @JsonProperty
  @NotNull
  private Duration step = Duration.ofSeconds(10);

  @JsonProperty
  @NotBlank
  private String environment;

  @JsonProperty
  @Positive
  private int maxPacketLength = 8932;

  @Override
  public Duration step() {
    return step;
  }

  public String getEnvironment() {
    return environment;
  }

  @Override
  public StatsdFlavor flavor() {
    return StatsdFlavor.DATADOG;
  }

  @Override
  public String get(final String key) {
    // We have no Micrometer key/value pairs to report, so always return `null`
    return null;
  }

  @Override
  public int maxPacketLength() {
    return maxPacketLength;
  }
}
