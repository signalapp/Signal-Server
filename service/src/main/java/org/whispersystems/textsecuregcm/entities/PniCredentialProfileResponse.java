/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.signal.libsignal.zkgroup.profiles.PniCredentialResponse;

public class PniCredentialProfileResponse extends CredentialProfileResponse {

  @JsonProperty
  @JsonSerialize(using = PniCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = PniCredentialResponseAdapter.Deserializing.class)
  @Nullable
  private PniCredentialResponse pniCredential;

  public PniCredentialProfileResponse() {
  }

  public PniCredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse,
      @Nullable final PniCredentialResponse pniCredential) {

    super(versionedProfileResponse);
    this.pniCredential = pniCredential;
  }

  @Nullable
  public PniCredentialResponse getPniCredential() {
    return pniCredential;
  }
}
