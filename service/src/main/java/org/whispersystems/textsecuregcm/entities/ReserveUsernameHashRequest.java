/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.util.List;

public record ReserveUsernameHashRequest(
    @Valid
    @Size(min=1, max=AccountController.MAXIMUM_USERNAME_HASHES_LIST_LENGTH)
    @JsonSerialize(contentUsing = ByteArrayBase64UrlAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayBase64UrlAdapter.Deserializing.class)
    List<byte[]> usernameHashes
) {}
