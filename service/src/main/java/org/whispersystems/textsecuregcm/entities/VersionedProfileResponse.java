/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;

@Schema(description = "Versioned profile containing encrypted fields. Versioned fields may be empty if the version was not found.")
public record VersionedProfileResponse(

    @Schema(description = "Base profile information")
    @JsonUnwrapped
    BaseProfileResponse baseProfileResponse,

    @Schema(description = "Encrypted profile name. Padded base64.")
    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] name,

    @Schema(description = "Encrypted about text. Padded base64.")
    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] about,

    @Schema(description = "Encrypted about emoji. Padded base64.")
    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] aboutEmoji,

    @Schema(description = "Avatar CDN path")
    @JsonProperty
    String avatar,

    @Schema(description = "Encrypted payment address. Padded base64.")
    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] paymentAddress,

    @Schema(description = "Encrypted phone number sharing preference. Padded base64.")
    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] phoneNumberSharing) {

}
