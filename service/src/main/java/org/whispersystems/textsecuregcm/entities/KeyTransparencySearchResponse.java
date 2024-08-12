/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import katie.SearchResponse;
import org.whispersystems.textsecuregcm.util.SearchResponseProtobufAdapter;

import javax.validation.constraints.NotNull;
import java.util.Optional;

public record KeyTransparencySearchResponse(
    @NotNull
    @JsonSerialize(using = SearchResponseProtobufAdapter.Serializer.class)
    @JsonDeserialize(using = SearchResponseProtobufAdapter.Deserializer.class)
    @Schema(description = "The search response for the aci search key")
    SearchResponse aciSearchResponse,

    @JsonSerialize(contentUsing = SearchResponseProtobufAdapter.Serializer.class)
    @JsonDeserialize(contentUsing = SearchResponseProtobufAdapter.Deserializer.class)
    @Schema(description = "The search response for the e164 search key")
    Optional<SearchResponse> e164SearchResponse,

    @JsonSerialize(contentUsing = SearchResponseProtobufAdapter.Serializer.class)
    @JsonDeserialize(contentUsing = SearchResponseProtobufAdapter.Deserializer.class)
    @Schema(description = "The search response for the username hash search key")
    Optional<SearchResponse> usernameHashSearchResponse
) {}
