/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public class VersionedProfile {

  @JsonProperty
  private String version;

  @JsonProperty
  private String name;

  @JsonProperty
  private String avatar;

  @JsonProperty
  private String aboutEmoji;

  @JsonProperty
  private String about;

  @JsonProperty
  private String paymentAddress;

  @JsonProperty
  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  private byte[] commitment;

  public VersionedProfile() {}

  public VersionedProfile(
      String version, String name, String avatar, String aboutEmoji, String about, String paymentAddress,
      byte[] commitment) {
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
}
