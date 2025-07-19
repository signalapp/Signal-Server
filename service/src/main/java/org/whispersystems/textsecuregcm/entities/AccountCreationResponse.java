/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.swagger.v3.oas.annotations.media.Schema;

public record AccountCreationResponse(

    @JsonUnwrapped
    AccountIdentityResponse identityResponse,

    @Schema(description = "If true, there was an existing account registered for this number")
    boolean reregistration) {
}
