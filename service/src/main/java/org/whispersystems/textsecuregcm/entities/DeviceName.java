/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

public record DeviceName(@JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                         @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                         @NotEmpty
                         @Size(max = 225)
                         byte[] deviceName) {
}
