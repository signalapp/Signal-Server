/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;

public record AccountIdentityResponse(
    @Schema(description = "the account identifier for this account")
    UUID uuid,

    @Schema(description = "the phone number associated with this account")
    String number,

    @Schema(description = "the account identifier for this account's phone-number identity")
    UUID pni,

    @Schema(description = "a hash of this account's username, if set")
    @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
    @Nullable byte[] usernameHash,

    @Schema(description = "this account's username link handle, if set")
    @Nullable UUID usernameLinkHandle,

    @Schema(description = "whether any of this account's devices support storage")
    boolean storageCapable,

    @Schema(description = "entitlements for this account and their current expirations")
    Entitlements entitlements) {
}
