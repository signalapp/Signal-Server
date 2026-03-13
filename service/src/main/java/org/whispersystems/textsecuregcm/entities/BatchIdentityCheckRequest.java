/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

@Schema(description = "Request to check identity key fingerprints for multiple accounts")
public record BatchIdentityCheckRequest(
    @Schema(description = "List of accounts to check")
    @Valid @NotNull @Size(max = 1000) List<Element> elements) {

  /**
   * @param uuid        account id or phone number id
   * @param fingerprint most significant 4 bytes of SHA-256 of the 33-byte identity key field (32-byte curve25519 public
   *                    key prefixed with 0x05)
   */
  @Schema(description = "A service identifier and expected identity key fingerprint")
  public record Element(
      @Schema(description = "Identifier (ACI or PNI)")
      @NotNull
      @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
      @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
      ServiceIdentifier uuid,

      @Schema(description = "Expected identity key fingerprint (4 bytes, most significant bytes of SHA-256 hash of 33-byte identity key field)")
      @NotNull
      @ExactlySize(4)
      byte[] fingerprint) {
  }
}
