/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NonNormalizedPhoneNumberResponse {

  private final String originalNumber;
  private final String normalizedNumber;

  @JsonCreator
  NonNormalizedPhoneNumberResponse(@JsonProperty("originalNumber") final String originalNumber,
      @JsonProperty("normalizedNumber") final String normalizedNumber) {

    this.originalNumber = originalNumber;
    this.normalizedNumber = normalizedNumber;
  }

  public String getOriginalNumber() {
    return originalNumber;
  }

  public String getNormalizedNumber() {
    return normalizedNumber;
  }
}
