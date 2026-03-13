/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

import java.util.List;
import java.util.Map;

@Schema(description = "Unversioned profile containing basic information")
public class BaseProfileResponse {

  @Schema(description = "The account's public identity key (unpadded base64)")
  @JsonProperty
  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
  private IdentityKey identityKey;

  @Schema(description = "Checksum for unidentified access authentication (padded base64)")
  @JsonProperty
  @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
  private byte[] unidentifiedAccess;

  @Schema(description = "Whether unidentified access is unrestricted for this account")
  @JsonProperty
  private boolean unrestrictedUnidentifiedAccess;

  @Schema(description = "Device capabilities and whether they are enabled")
  @JsonProperty
  private Map<String, Boolean> capabilities;

  @Schema(description = "List of badges displayed on the profile")
  @JsonProperty
  private List<Badge> badges;

  @Schema(description = "Service identifier (ACI or PNI)")
  @JsonProperty
  @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
  @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
  private ServiceIdentifier uuid;

  public BaseProfileResponse() {
  }

  public BaseProfileResponse(final IdentityKey identityKey,
      final byte[] unidentifiedAccess,
      final boolean unrestrictedUnidentifiedAccess,
      final Map<String, Boolean> capabilities,
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

  public byte[] getUnidentifiedAccess() {
    return unidentifiedAccess;
  }

  public boolean isUnrestrictedUnidentifiedAccess() {
    return unrestrictedUnidentifiedAccess;
  }

  public Map<String, Boolean> getCapabilities() {
    return capabilities;
  }

  public List<Badge> getBadges() {
    return badges;
  }

  public ServiceIdentifier getUuid() {
    return uuid;
  }
}
