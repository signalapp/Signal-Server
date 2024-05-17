/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

@Schema(description = """
    A time limited external service credential that can be used to authenticate and restore from SVR3.
    """)
public record Svr3Credentials(

    @Schema(description = "The credential username")
    String username,

    @Schema(description = "The credential password")
    String password,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
        If present, a shareSet previously stored for this account via /v3/backups/shareSet. Required to restore a value
        from SVR3. Encoded in standard un-padded base64.
        """, implementation = String.class)
    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @Nullable byte[] shareSet) {}
