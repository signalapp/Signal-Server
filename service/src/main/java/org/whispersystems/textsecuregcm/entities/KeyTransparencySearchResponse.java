/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import javax.validation.constraints.NotNull;
import java.util.Optional;

public record KeyTransparencySearchResponse(
    @NotNull
    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The search response for the aci search key encoded in standard un-padded base64")
    byte[] aciSearchResponse,

    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The search response for the e164 search key encoded in standard un-padded base64")
    Optional<byte[]> e164SearchResponse,

    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The search response for the username hash search key encoded in standard un-padded base64")
    Optional<byte[]> usernameHashSearchResponse
) {}
