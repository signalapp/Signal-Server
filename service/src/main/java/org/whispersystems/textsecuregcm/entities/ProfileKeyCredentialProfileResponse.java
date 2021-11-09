/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialResponse;
import javax.annotation.Nullable;

public class ProfileKeyCredentialProfileResponse extends CredentialProfileResponse {

  @JsonProperty
  @JsonSerialize(using = ProfileKeyCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = ProfileKeyCredentialResponseAdapter.Deserializing.class)
  @Nullable
  private ProfileKeyCredentialResponse credential;

  public ProfileKeyCredentialProfileResponse() {
  }

  public ProfileKeyCredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse,
      @Nullable final ProfileKeyCredentialResponse credential) {

    super(versionedProfileResponse);
    this.credential = credential;
  }

  @Nullable
  public ProfileKeyCredentialResponse getCredential() {
    return credential;
  }
}
