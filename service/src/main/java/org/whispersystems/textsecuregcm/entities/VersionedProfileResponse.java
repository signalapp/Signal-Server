/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;

public record VersionedProfileResponse(

    @JsonUnwrapped
    BaseProfileResponse baseProfileResponse,

    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] name,

    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] about,

    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] aboutEmoji,

    @JsonProperty
    String avatar,

    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] paymentAddress,

    @JsonProperty
    @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
    byte[] phoneNumberSharing) {

}
