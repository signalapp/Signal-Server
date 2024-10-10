/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;

public record LinkDeviceToken(
    @Schema(description = """
        An opaque token to send to a new linked device that authorizes the new device to link itself to the account that
        requested this token.
        """)
    @JsonProperty("verificationCode") String token,

    @Schema(description = """
        An opaque identifier for the generated token that the caller may use to watch for a new device to complete the
        linking process.
        """)
    String tokenIdentifier) {
}
