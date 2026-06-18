/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;


import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public record CreateDonationPermitsRequest(
    @Schema(description = "A serialized DonationPermitRequest")
    @NotEmpty
    @NotNull
    byte[] permitRequest) {
}
