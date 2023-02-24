/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.util.E164;

public record AuthCheckRequest(@Schema(description = "The e164-formatted phone number.")
                               @NotNull @E164 String number,
                               @Schema(description = "A list of SVR auth values, previously retrieved from `/v1/backup/auth`; may contain at most 10.")
                               @NotEmpty @Size(max = 10) List<String> passwords) {
}
