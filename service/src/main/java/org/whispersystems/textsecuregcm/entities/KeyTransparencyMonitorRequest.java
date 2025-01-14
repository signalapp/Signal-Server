/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.Optional;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record KeyTransparencyMonitorRequest(

    @Valid
    @NotNull
    AciMonitor aci,

    @Valid
    @NotNull
    Optional<@Valid E164Monitor> e164,

    @Valid
    @NotNull
    Optional<@Valid UsernameHashMonitor> usernameHash,

    @Schema(description = "The tree head size to prove consistency against.")
    @Positive long lastNonDistinguishedTreeHeadSize,

    @Schema(description = "The distinguished tree head size to prove consistency against.")
    @Positive long lastDistinguishedTreeHeadSize
) {

  public record AciMonitor(
      @NotNull
      @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
      @JsonDeserialize(using = ServiceIdentifierAdapter.AciServiceIdentifierDeserializer.class)
      @Schema(description = "The aci identifier to monitor")
      AciServiceIdentifier value,

      @Schema(description = "A log tree position maintained by the client for the aci.")
      @Positive
      long entryPosition,

      @Schema(description = "The commitment index derived from a previous search request, encoded in standard unpadded base64")
      @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(32)
      byte[] commitmentIndex
  ) {}

  public record E164Monitor(
      @Schema(description = "The e164-formatted phone number to monitor")
      @NotBlank
      String value,

      @Schema(description = "A log tree position maintained by the client for the e164.")
      @Positive
      long entryPosition,

      @Schema(description = "The commitment index derived from a previous search request, encoded in standard unpadded base64")
      @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(32)
      byte[] commitmentIndex
  ) {}

  public record UsernameHashMonitor(

      @Schema(description = "The username hash to monitor, encoded in url-safe unpadded base64.")
      @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
      @NotNull
      @NotEmpty
      byte[] value,

      @Schema(description = "A log tree position maintained by the client for the username hash.")
      @Positive
      long entryPosition,

      @Schema(description = "The commitment index derived from a previous search request, encoded in standard unpadded base64")
      @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(32)
      byte[] commitmentIndex
  ) {}
}
