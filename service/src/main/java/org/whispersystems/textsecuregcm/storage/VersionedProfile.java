/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import java.util.Arrays;
import java.util.Objects;

public class VersionedProfile {

  private final String version;
  private final String name;
  private final String avatar;
  private final String aboutEmoji;
  private final String about;
  private final String paymentAddress;

  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  private byte[] commitment;

  @JsonCreator
  public VersionedProfile(
      @JsonProperty("version") final String version,
      @JsonProperty("name") final String name,
      @JsonProperty("avatar") final String avatar,
      @JsonProperty("aboutEmoji") final String aboutEmoji,
      @JsonProperty("about") final String about,
      @JsonProperty("paymentAddress") final String paymentAddress,
      @JsonProperty("commitment") final byte[] commitment) {
    this.version = version;
    this.name = name;
    this.avatar = avatar;
    this.aboutEmoji = aboutEmoji;
    this.about = about;
    this.paymentAddress = paymentAddress;
    this.commitment = commitment;
  }

  public String getVersion() {
    return version;
  }

  public String getName() {
    return name;
  }

  public String getAvatar() {
    return avatar;
  }

  public String getAboutEmoji() {
    return aboutEmoji;
  }

  public String getAbout() {
    return about;
  }

  public String getPaymentAddress() {
    return paymentAddress;
  }

  public byte[] getCommitment() {
    return commitment;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final VersionedProfile that = (VersionedProfile) o;
    return Objects.equals(version, that.version) && Objects.equals(name, that.name) && Objects.equals(avatar,
        that.avatar) && Objects.equals(aboutEmoji, that.aboutEmoji) && Objects.equals(about, that.about)
        && Objects.equals(paymentAddress, that.paymentAddress) && Arrays.equals(commitment, that.commitment);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(version, name, avatar, aboutEmoji, about, paymentAddress);
    result = 31 * result + Arrays.hashCode(commitment);
    return result;
  }
}
