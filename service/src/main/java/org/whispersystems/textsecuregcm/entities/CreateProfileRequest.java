/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Optional;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ValidHexString;

public record CreateProfileRequest(
  @JsonProperty
  @NotNull
  @JsonDeserialize(using = ProfileKeyCommitmentAdapter.Deserializing.class)
  @JsonSerialize(using = ProfileKeyCommitmentAdapter.Serializing.class)
  ProfileKeyCommitment commitment,

  @JsonProperty
  @NotEmpty
  @ValidHexString
  @ExactlySize({64})
  String version,

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({81, 285})
  byte[] name,

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 60})
  byte[] aboutEmoji,

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 156, 282, 540})
  byte[] about,

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 582})
  byte[] paymentAddress,

  @JsonProperty("avatar")
  boolean hasAvatar,

  @JsonProperty
  boolean sameAvatar,

  @JsonProperty("badgeIds")
  Optional<List<String>> badges,

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 29})
  byte[] phoneNumberSharing
) {

  public enum AvatarChange {
    UNCHANGED,
    CLEAR,
    UPDATE;
  }

  public AvatarChange getAvatarChange() {
    if (!hasAvatar()) {
      return AvatarChange.CLEAR;
    }
    if (!sameAvatar) {
      return AvatarChange.UPDATE;
    }
    return AvatarChange.UNCHANGED;
  }

}
