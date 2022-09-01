/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.Min;
import java.time.Duration;

public class UsernameConfiguration {

  @JsonProperty
  @Min(1)
  private int discriminatorInitialWidth = 4;

  @JsonProperty
  @Min(1)
  private int discriminatorMaxWidth = 9;

  @JsonProperty
  @Min(1)
  private int attemptsPerWidth = 10;

  @JsonProperty
  private Duration reservationTtl = Duration.ofMinutes(5);

  public int getDiscriminatorInitialWidth() {
    return discriminatorInitialWidth;
  }

  public int getDiscriminatorMaxWidth() {
    return discriminatorMaxWidth;
  }

  public int getAttemptsPerWidth() {
    return attemptsPerWidth;
  }

  public Duration getReservationTtl() {
    return reservationTtl;
  }
}
