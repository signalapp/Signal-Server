/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.E164;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.util.Optional;

public record KeyTransparencySearchRequest(
    @NotNull
    @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
    @JsonDeserialize(using = ServiceIdentifierAdapter.AciServiceIdentifierDeserializer.class)
    @Schema(description = "The aci identifier to look up")
    AciServiceIdentifier aci,

    @E164
    @Schema(description = "The e164-formatted phone number to look up")
    Optional<String> e164,

    @JsonSerialize(contentUsing = ByteArrayBase64UrlAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayBase64UrlAdapter.Deserializing.class)
    @Schema(description = "The username hash to look up, encoded in web-safe unpadded base64.")
    Optional<byte[]> usernameHash,

    @Schema(description = "The tree head size to prove consistency against.")
    Optional<@Positive Long> lastTreeHeadSize
) {}
