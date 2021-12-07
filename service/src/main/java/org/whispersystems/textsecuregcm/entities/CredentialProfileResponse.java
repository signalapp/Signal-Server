/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

public abstract class CredentialProfileResponse {

  @JsonUnwrapped
  private VersionedProfileResponse versionedProfileResponse;

  protected CredentialProfileResponse() {
  }

  protected CredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse) {
    this.versionedProfileResponse = versionedProfileResponse;
  }

  public VersionedProfileResponse getVersionedProfileResponse() {
    return versionedProfileResponse;
  }
}
