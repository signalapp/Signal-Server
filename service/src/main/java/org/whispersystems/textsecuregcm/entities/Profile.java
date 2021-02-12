/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.UUID;
import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;
import org.whispersystems.textsecuregcm.storage.PaymentAddress;

public class Profile {

  @JsonProperty
  private String identityKey;

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

  @JsonProperty
  private String unidentifiedAccess;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private UserCapabilities capabilities;

  @JsonProperty
  private String username;

  @JsonProperty
  private UUID uuid;

  @JsonProperty
  private List<PaymentAddress> payments;

  @JsonProperty
  @JsonSerialize(using = ProfileKeyCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = ProfileKeyCredentialResponseAdapter.Deserializing.class)
  private ProfileKeyCredentialResponse credential;

  public Profile() {}

  public Profile(
      String name, String about, String aboutEmoji, String avatar, String paymentAddress, String identityKey,
      String unidentifiedAccess, boolean unrestrictedUnidentifiedAccess, UserCapabilities capabilities, String username,
      UUID uuid, ProfileKeyCredentialResponse credential, List<PaymentAddress> payments)
  {
    this.name = name;
    this.about = about;
    this.aboutEmoji = aboutEmoji;
    this.avatar = avatar;
    this.paymentAddress = paymentAddress;
    this.identityKey = identityKey;
    this.unidentifiedAccess = unidentifiedAccess;
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
    this.capabilities = capabilities;
    this.username = username;
    this.uuid = uuid;
    this.payments = payments;
    this.credential = credential;
  }

  @VisibleForTesting
  public String getIdentityKey() {
    return identityKey;
  }

  @VisibleForTesting
  public String getName() {
    return name;
  }

  public String getAbout() {
    return about;
  }

  public String getAboutEmoji() {
    return aboutEmoji;
  }

  @VisibleForTesting
  public String getAvatar() {
    return avatar;
  }

  public String getPaymentAddress() {
    return paymentAddress;
  }

  @VisibleForTesting
  public String getUnidentifiedAccess() {
    return unidentifiedAccess;
  }

  @VisibleForTesting
  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

  @VisibleForTesting
  public UserCapabilities getCapabilities() {
    return capabilities;
  }

  @VisibleForTesting
  public String getUsername() {
    return username;
  }

  @VisibleForTesting
  public UUID getUuid() {
    return uuid;
  }

  @VisibleForTesting
  public List<PaymentAddress> getPayments() {
    return payments;
  }
}
