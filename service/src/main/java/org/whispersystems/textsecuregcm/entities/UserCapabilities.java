/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(
    @JsonProperty("gv1-migration") boolean gv1Migration,
    boolean senderKey,
    boolean announcementGroup,
    boolean changeNumber,
    boolean stories,
    boolean giftBadges,
    boolean paymentActivation) {

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
}
