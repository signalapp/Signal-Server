/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

public class ChangePhoneNumberRequest {

  @JsonProperty
  @NotBlank
  final String number;

  @JsonProperty
  @NotBlank
  final String code;

  @JsonProperty("reglock")
  @Nullable
  final String registrationLock;

  @JsonCreator
  public ChangePhoneNumberRequest(@JsonProperty("number") final String number,
      @JsonProperty("code") final String code,
      @JsonProperty("reglock") @Nullable final String registrationLock) {

    this.number = number;
    this.code = code;
    this.registrationLock = registrationLock;
  }

  public String getNumber() {
    return number;
  }

  public String getCode() {
    return code;
  }

  @Nullable
  public String getRegistrationLock() {
    return registrationLock;
  }
}
