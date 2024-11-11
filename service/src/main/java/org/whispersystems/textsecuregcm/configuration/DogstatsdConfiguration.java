/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.micrometer.statsd.StatsdFlavor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

@JsonTypeName("default")
public class DogstatsdConfiguration implements DatadogConfiguration {

  @JsonProperty
  @NotNull
  private Duration step = Duration.ofSeconds(10);

  @JsonProperty
  @NotBlank
  private String environment;

  @JsonProperty
  @NotBlank
  private String host;

  @Override
  public Duration step() {
    return step;
  }

  @Override
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
  public String host() {
    return host;
  }

  @Override
  public Duration getShutdownWaitDuration() {
    return step().plus(step.dividedBy(2));
  }
}
