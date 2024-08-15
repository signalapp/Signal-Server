/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.util.List;
import java.util.Optional;

public record KeyTransparencyMonitorRequest(
    @NotNull
    @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
    @JsonDeserialize(using = ServiceIdentifierAdapter.AciServiceIdentifierDeserializer.class)
    @Schema(description = "The aci identifier to monitor")
    AciServiceIdentifier aci,

    @NotEmpty
    @Schema(description = "A list of log tree positions maintained by the client for the aci search key.")
    List<@Positive Long> aciPositions,

    @Schema(description = "The e164-formatted phone number to monitor")
    Optional<String> e164,

    @Schema(description = "A list of log tree positions maintained by the client for the e164 search key.")
    Optional<List<@Positive Long>> e164Positions,

    @JsonSerialize(contentUsing = ByteArrayBase64UrlAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayBase64UrlAdapter.Deserializing.class)
    @Schema(description = "The username hash to monitor, encoded in url-safe unpadded base64.")
    Optional<byte[]> usernameHash,

    @Schema(description = "A list of log tree positions maintained by the client for the username hash search key.")
    Optional<List<@Positive Long>> usernameHashPositions,

    @Schema(description = "The tree head size to prove consistency against.")
    Optional<@Positive Long> lastNonDistinguishedTreeHeadSize,

    @Schema(description = "The distinguished tree head size to prove consistency against.")
    Optional<@Positive Long> lastDistinguishedTreeHeadSize
) {

  @AssertTrue
  public boolean isUsernameHashFieldsValid() {
    return (usernameHash.isEmpty() && usernameHashPositions.isEmpty()) ||
        (usernameHash.isPresent() && usernameHashPositions.isPresent() && !usernameHashPositions.get().isEmpty());
  }

  @AssertTrue
  public boolean isE164VFieldsValid() {
    return (e164.isEmpty() && e164Positions.isEmpty()) ||
        (e164.isPresent() && e164Positions.isPresent() && !e164Positions.get().isEmpty());
  }
}
