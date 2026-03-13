/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;

@Schema(description = "Profile response with an expiring profile key credential")
public class ExpiringProfileKeyCredentialProfileResponse {

  @JsonUnwrapped
  private VersionedProfileResponse versionedProfileResponse;

  @Schema(description = "Expiring profile key credential response. Null if profile version was not found")
  @JsonProperty
  @JsonSerialize(using = ExpiringProfileKeyCredentialResponseAdapter.Serializing.class)
  @JsonDeserialize(using = ExpiringProfileKeyCredentialResponseAdapter.Deserializing.class)
  @Nullable
  private ExpiringProfileKeyCredentialResponse credential;

  public ExpiringProfileKeyCredentialProfileResponse() {
  }

  public ExpiringProfileKeyCredentialProfileResponse(final VersionedProfileResponse versionedProfileResponse,
      @Nullable final ExpiringProfileKeyCredentialResponse credential) {

    this.versionedProfileResponse = versionedProfileResponse;
    this.credential = credential;
  }

  @Nullable
  public ExpiringProfileKeyCredentialResponse getCredential() {
    return credential;
  }

  public VersionedProfileResponse getVersionedProfileResponse() {
    return versionedProfileResponse;
  }
}
