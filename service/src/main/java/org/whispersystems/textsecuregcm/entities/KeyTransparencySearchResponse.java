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
    @Schema(description = "The `FullTreeHead` protobuf encoded in standard un-padded base64. This should be used across all identifiers.")
    byte[] fullTreeHead,

    @NotNull
    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The `TreeSearchResponse` protobuf for the ACI identifier encoded in standard un-padded base64")
    byte[] aciSearchResponse,

    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The `TreeSearchResponse` protobuf for the E164 encoded in standard un-padded base64")
    Optional<byte[]> e164SearchResponse,

    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "The `TreeSearchResponse` protobuf for the username hash encoded in standard un-padded base64")
    Optional<byte[]> usernameHashSearchResponse
) {}
