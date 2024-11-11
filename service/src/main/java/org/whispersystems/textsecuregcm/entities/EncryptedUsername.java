/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;

public record EncryptedUsername(
    @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
    @NotNull
    @Size(min = 1, max = EncryptedUsername.MAX_SIZE)
    @Schema(type = "string", description = "the URL-safe base64 encoding of the encrypted username")
    byte[] usernameLinkEncryptedValue,

    @JsonProperty
    @Schema(type = "boolean", description = "if set and the account already has an encrypted-username link handle, reuse the same link handle rather than generating a new one. The response will still have the link handle.")
    boolean keepLinkHandle
) {

  public static final int MAX_SIZE = 128;

  public EncryptedUsername(final byte[] usernameLinkEncryptedValue) {
    this(usernameLinkEncryptedValue, false);
  }

}
