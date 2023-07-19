/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.UUID;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import io.swagger.v3.oas.annotations.media.Schema;

public record UsernameLinkHandle(
    @Schema(description = "A handle that can be included in username links to retrieve the stored encrypted username")
    @NotNull
    UUID usernameLinkHandle) {
}
