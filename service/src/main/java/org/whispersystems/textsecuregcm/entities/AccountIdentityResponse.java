/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import java.util.UUID;
import javax.annotation.Nullable;

public record AccountIdentityResponse(UUID uuid,
                                      String number,
                                      UUID pni,
                                      @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
                                      @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
                                      @Nullable byte[] usernameHash,
                                      boolean storageCapable) {
}
