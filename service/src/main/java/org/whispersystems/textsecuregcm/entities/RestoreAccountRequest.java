/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

@Schema(description = """
    Represents a request from a new device to restore account data by some method.
    """)
public record RestoreAccountRequest(
    @NotNull
    @Schema(description = "The method by which the new device has requested account data restoration")
    Method method,

    @Schema(description = "Additional data to use to bootstrap a connection between devices, in standard unpadded base64.",
        implementation = String.class)
    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Size(max = 4096)
    @Nullable byte[] deviceTransferBootstrap) {

  public enum Method {
    @Schema(description = "Restore account data from a remote message history backup")
    REMOTE_BACKUP,

    @Schema(description = "Restore account data from a local backup archive")
    LOCAL_BACKUP,

    @Schema(description = "Restore account data via direct device-to-device transfer")
    DEVICE_TRANSFER,

    @Schema(description = "Do not restore account data")
    DECLINE,
  }
}
