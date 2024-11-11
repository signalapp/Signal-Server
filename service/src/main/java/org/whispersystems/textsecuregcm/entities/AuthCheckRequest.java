/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.whispersystems.textsecuregcm.util.E164;

public record AuthCheckRequest(@Schema(description = "The e164-formatted phone number.")
                               @NotNull @E164 String number,
                               @Schema(description = """
                               A list of SVR tokens, previously retrieved from `backup/auth`. Tokens should be the
                               of the form "username:password". May contain at most 10 tokens.""")
                               @JsonProperty("tokens")
                               @JsonAlias("passwords") // deprecated
                               @NotEmpty @Size(max = 10) List<String> tokens) {
}
