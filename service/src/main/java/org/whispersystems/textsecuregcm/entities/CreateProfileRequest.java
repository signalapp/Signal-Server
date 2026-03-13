/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Optional;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ValidHexString;

@Schema(description = "Request to create or update a versioned profile")
public record CreateProfileRequest(
  @Schema(description = "Profile key commitment")
  @JsonProperty
  @NotNull
  @JsonDeserialize(using = ProfileKeyCommitmentAdapter.Deserializing.class)
  @JsonSerialize(using = ProfileKeyCommitmentAdapter.Serializing.class)
  ProfileKeyCommitment commitment,

  @Schema(description = "Profile version identifier (hex-encoding of the public key)")
  @JsonProperty
  @NotEmpty
  @ValidHexString
  @ExactlySize({64})
  String version,

  @Schema(description = "Encrypted profile name. Padded base64.")
  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({81, 285})
  byte[] name,

  @Schema(description = "Encrypted about emoji. Padded base64.")
  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 60})
  byte[] aboutEmoji,

  @Schema(description = "Encrypted about text. Padded base64.")
  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 156, 282, 540})
  byte[] about,

  @Schema(description = "Encrypted payment address. Padded base64.")
  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 582})
  byte[] paymentAddress,

  @Schema(description = "Whether the profile has an avatar")
  @JsonProperty("avatar")
  boolean hasAvatar,

  @Schema(description = "Whether the avatar is unchanged from the previous version")
  @JsonProperty
  boolean sameAvatar,

  @Schema(description = "List of badge IDs to display on the profile")
  @JsonProperty("badgeIds")
  Optional<List<String>> badges,

  @Schema(description = "Encrypted phone number sharing preference. Padded base64.")
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
