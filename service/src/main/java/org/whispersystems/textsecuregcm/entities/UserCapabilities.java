/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.storage.Account;

public class UserCapabilities {

  public static UserCapabilities createForAccount(Account account) {
    return new UserCapabilities(
        account.isGroupsV2Supported(),
        account.isGv1MigrationSupported(),
        account.isSenderKeySupported(),
        account.isAnnouncementGroupSupported());
  }

  @JsonProperty
  private boolean gv2;

  @JsonProperty("gv1-migration")
  private boolean gv1Migration;

  @JsonProperty
  private boolean senderKey;

  @JsonProperty
  private boolean announcementGroup;

  public UserCapabilities() {}

  public UserCapabilities(boolean gv2, boolean gv1Migration, final boolean senderKey, final boolean announcementGroup) {
    this.gv2  = gv2;
    this.gv1Migration = gv1Migration;
    this.senderKey = senderKey;
    this.announcementGroup = announcementGroup;
  }

  public boolean isGv2() {
    return gv2;
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
}
