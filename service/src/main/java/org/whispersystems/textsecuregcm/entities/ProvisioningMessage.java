/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;

public record ProvisioningMessage(
    @Schema(description = "The MIME base64-encoded body of the provisioning message to send to the destination device")
    @NotEmpty
    String body) {
}
