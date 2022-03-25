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
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class CreateProfileRequest {

  @JsonProperty
  @NotEmpty
  private String version;

  @JsonProperty
  @ExactlySize({108, 380})
  private String name;

  @JsonProperty
  private boolean avatar;

  @JsonProperty
  private boolean sameAvatar;

  @JsonProperty
  @ExactlySize({0, 80})
  private String aboutEmoji;

  @JsonProperty
  @ExactlySize({0, 208, 376, 720})
  private String about;

  @JsonProperty
  @ExactlySize({0, 776})
  private String paymentAddress;

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
      ProfileKeyCommitment commitment, String version, String name, String aboutEmoji, String about,
      String paymentAddress, boolean wantsAvatar, boolean sameAvatar, List<String> badgeIds) {
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

  public String getName() {
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

  public String getAboutEmoji() {
    return StringUtils.stripToNull(aboutEmoji);
  }

  public String getAbout() {
    return StringUtils.stripToNull(about);
  }

  public String getPaymentAddress() {
    return StringUtils.stripToNull(paymentAddress);
  }

  public Optional<List<String>> getBadges() {
    return Optional.ofNullable(badgeIds);
  }
}
