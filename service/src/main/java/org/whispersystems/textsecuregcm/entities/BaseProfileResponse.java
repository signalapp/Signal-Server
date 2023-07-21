/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

import java.util.List;

public class BaseProfileResponse {

  @JsonProperty
  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
  private IdentityKey identityKey;

  @JsonProperty
  private String unidentifiedAccess;

  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @JsonProperty
  private UserCapabilities capabilities;

  @JsonProperty
  private List<Badge> badges;

  @JsonProperty
  @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
  @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
  private ServiceIdentifier uuid;

  public BaseProfileResponse() {
  }

  public BaseProfileResponse(final IdentityKey identityKey,
      final String unidentifiedAccess,
      final boolean unrestrictedUnidentifiedAccess,
      final UserCapabilities capabilities,
      final List<Badge> badges,
      final ServiceIdentifier uuid) {

    this.identityKey = identityKey;
    this.unidentifiedAccess = unidentifiedAccess;
    this.unrestrictedUnidentifiedAccess = unrestrictedUnidentifiedAccess;
    this.capabilities = capabilities;
    this.badges = badges;
    this.uuid = uuid;
  }

  public IdentityKey getIdentityKey() {
    return identityKey;
  }

  public String getUnidentifiedAccess() {
    return unidentifiedAccess;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

  public UserCapabilities getCapabilities() {
    return capabilities;
  }

  public List<Badge> getBadges() {
    return badges;
  }

  public ServiceIdentifier getUuid() {
    return uuid;
  }
}
