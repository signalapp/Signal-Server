/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ValidBase64URLString;

public record RemoteAttachment(
    @Schema(description = "The attachment cdn")
    @NotNull
    Integer cdn,

    @NotBlank
    @ValidBase64URLString
    @Schema(description = "The attachment key")
    String key) {
}
