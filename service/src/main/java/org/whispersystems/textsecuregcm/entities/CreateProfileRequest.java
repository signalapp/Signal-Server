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
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class CreateProfileRequest {

  @JsonProperty
  @NotEmpty
  private String version;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({81, 285})
  private byte[] name;

  @JsonProperty
  private boolean avatar;

  @JsonProperty
  private boolean sameAvatar;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 60})
  private byte[] aboutEmoji;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 156, 282, 540})
  private byte[] about;

  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  @ExactlySize({0, 582})
  private byte[] paymentAddress;

  @JsonProperty
  @Nullable
  private List<String> badgeIds;

  @JsonProperty
  @NotNull
  @JsonDeserialize(using = ProfileKeyCommitmentAdapter.Deserializing.class)
  @JsonSerialize(using = ProfileKeyCommitmentAdapter.Serializing.class)
  private ProfileKeyCommitment commitment;

  public CreateProfileRequest() {
  }

  public CreateProfileRequest(
      final ProfileKeyCommitment commitment, final String version, final byte[] name, final byte[] aboutEmoji, final byte[] about,
      final byte[] paymentAddress, final boolean wantsAvatar, final boolean sameAvatar, final List<String> badgeIds) {
    this.commitment = commitment;
    this.version = version;
    this.name = name;
    this.aboutEmoji = aboutEmoji;
    this.about = about;
    this.paymentAddress = paymentAddress;
    this.avatar = wantsAvatar;
    this.sameAvatar = sameAvatar;
    this.badgeIds = badgeIds;
  }

  public ProfileKeyCommitment getCommitment() {
    return commitment;
  }

  public String getVersion() {
    return version;
  }

  public byte[] getName() {
    return name;
  }

  public boolean hasAvatar() {
    return avatar;
  }

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

  public byte[] getAboutEmoji() {
    return aboutEmoji;
  }

  public byte[] getAbout() {
    return about;
  }

  public byte[] getPaymentAddress() {
    return paymentAddress;
  }

  public Optional<List<String>> getBadges() {
    return Optional.ofNullable(badgeIds);
  }
}
