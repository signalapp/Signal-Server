/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

public class VersionedProfileResponse {

  @JsonUnwrapped
  private BaseProfileResponse baseProfileResponse;

  @JsonProperty
  private String name;

  @JsonProperty
  private String about;

  @JsonProperty
  private String aboutEmoji;

  @JsonProperty
  private String avatar;

  @JsonProperty
  private String paymentAddress;

  public VersionedProfileResponse() {
  }

  public VersionedProfileResponse(final BaseProfileResponse baseProfileResponse,
      final String name,
      final String about,
      final String aboutEmoji,
      final String avatar,
      final String paymentAddress) {

    this.baseProfileResponse = baseProfileResponse;
    this.name = name;
    this.about = about;
    this.aboutEmoji = aboutEmoji;
    this.avatar = avatar;
    this.paymentAddress = paymentAddress;
  }

  public BaseProfileResponse getBaseProfileResponse() {
    return baseProfileResponse;
  }

  public String getName() {
    return name;
  }

  public String getAbout() {
    return about;
  }

  public String getAboutEmoji() {
    return aboutEmoji;
  }

  public String getAvatar() {
    return avatar;
  }

  public String getPaymentAddress() {
    return paymentAddress;
  }
}
