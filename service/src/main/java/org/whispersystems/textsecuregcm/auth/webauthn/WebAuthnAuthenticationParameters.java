/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import java.util.List;

public record WebAuthnAuthenticationParameters(
    @Schema(description = "The authentication ceremony challenge.")
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    byte[] challenge,

    @Schema(description = "The number of seconds the challenge will be valid.")
    long timeoutSeconds,

    @Schema(description = "A list of authenticators previously registered to the account.")
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    List<byte[]> allowedCredentialIds) {}
