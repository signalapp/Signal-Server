/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.zkgroup.profiles.PniCredentialResponse;
import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class ProfileKeyCredentialProfileResponse extends CredentialProfileResponse {

  @JsonProperty
  @JsonSerialize(using = ProfileKeyCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = ProfileKeyCredentialResponseAdapter.Deserializing.class)
  private ProfileKeyCredentialResponse credential;

  public ProfileKeyCredentialProfileResponse() {
  }

  public ProfileKeyCredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse,
      final ProfileKeyCredentialResponse credential) {

    super(versionedProfileResponse);
    this.credential = credential;
  }

  public ProfileKeyCredentialResponse getCredential() {
    return credential;
  }
}
