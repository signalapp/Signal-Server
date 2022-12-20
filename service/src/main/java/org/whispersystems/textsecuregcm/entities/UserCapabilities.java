/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.storage.Account;

public class UserCapabilities {

  public static UserCapabilities createForAccount(Account account) {
    return new UserCapabilities(
        true,
        account.isSenderKeySupported(),
        account.isAnnouncementGroupSupported(),
        account.isChangeNumberSupported(),
        account.isStoriesSupported(),
        account.isGiftBadgesSupported(),
        false); // Hardcode to false until all clients support the flow
  }

  @JsonProperty("gv1-migration")
  private boolean gv1Migration;

  @JsonProperty
  private boolean senderKey;

  @JsonProperty
  private boolean announcementGroup;

  @JsonProperty
  private boolean changeNumber;

  @JsonProperty
  private boolean stories;

  @JsonProperty
  private boolean giftBadges;

  @JsonProperty
  private boolean paymentActivation;

  public UserCapabilities() {
  }

  public UserCapabilities(
      boolean gv1Migration,
      final boolean senderKey,
      final boolean announcementGroup,
      final boolean changeNumber,
      final boolean stories,
      final boolean giftBadges,
      final boolean paymentActivation) {

    this.gv1Migration = gv1Migration;
    this.senderKey = senderKey;
    this.announcementGroup = announcementGroup;
    this.changeNumber = changeNumber;
    this.stories = stories;
    this.giftBadges = giftBadges;
    this.paymentActivation = paymentActivation;
  }

  public boolean isGv1Migration() {
    return gv1Migration;
  }

  public boolean isSenderKey() {
    return senderKey;
  }

  public boolean isAnnouncementGroup() {
    return announcementGroup;
  }

  public boolean isChangeNumber() {
    return changeNumber;
  }

  public boolean isStories() {
    return stories;
  }

  public boolean isGiftBadges() {
    return giftBadges;
  }

  public boolean isPaymentActivation() { return paymentActivation; }
}
