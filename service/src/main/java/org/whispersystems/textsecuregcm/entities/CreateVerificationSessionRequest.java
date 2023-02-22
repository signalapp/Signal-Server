/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.whispersystems.textsecuregcm.util.E164;

// Not a record, because Jackson does not support @JsonUnwrapped with records
// https://github.com/FasterXML/jackson-databind/issues/1497
public final class CreateVerificationSessionRequest {

  @E164
  @NotBlank
  @JsonProperty
  private String number;

  @Valid
  @JsonUnwrapped
  private UpdateVerificationSessionRequest updateVerificationSessionRequest;

  public String getNumber() {
    return number;
  }

  public UpdateVerificationSessionRequest getUpdateVerificationSessionRequest() {
    return updateVerificationSessionRequest;
  }

}
