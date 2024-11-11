/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public class ReportMessageConfiguration {

  @JsonProperty
  @NotNull
  private final Duration reportTtl = Duration.ofDays(7);

  @JsonProperty
  @NotNull
  private final Duration counterTtl = Duration.ofDays(1);

  public Duration getReportTtl() {
    return reportTtl;
  }

  public Duration getCounterTtl() {
    return counterTtl;
  }
}
