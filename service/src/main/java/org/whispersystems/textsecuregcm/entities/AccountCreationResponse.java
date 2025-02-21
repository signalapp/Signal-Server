/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.swagger.v3.oas.annotations.media.Schema;

// Note, this class cannot be converted into a record because @JsonUnwrapped does not work with records until 2.19. We are on 2.18 until Dropwizard’s BOM updates.
// https://github.com/FasterXML/jackson-databind/issues/1467
public class AccountCreationResponse {

  @JsonUnwrapped
  private AccountIdentityResponse identityResponse;

  @Schema(description = "If true, there was an existing account registered for this number")
  private boolean reregistration;

  public AccountCreationResponse() {
  }

  public AccountCreationResponse(AccountIdentityResponse identityResponse, boolean reregistration) {
    this.identityResponse = identityResponse;
    this.reregistration = reregistration;
  }

  public boolean isReregistration() {
    return reregistration;
  }
}
