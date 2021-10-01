/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class GroupCredentials {

  private final List<GroupCredential> credentials;

  @JsonCreator
  public GroupCredentials(
      @JsonProperty("credentials") List<GroupCredential> credentials) {
    this.credentials = credentials;
  }

  public List<GroupCredential> getCredentials() {
    return credentials;
  }

  public static class GroupCredential {

    private final byte[] credential;
    private final int redemptionTime;

    @JsonCreator
    public GroupCredential(
        @JsonProperty("credential") byte[] credential,
        @JsonProperty("redemptionTime") int redemptionTime) {
      this.credential = credential;
      this.redemptionTime = redemptionTime;
    }

    public byte[] getCredential() {
      return credential;
    }

    public int getRedemptionTime() {
      return redemptionTime;
    }
  }
}
