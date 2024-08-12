/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import katie.FullTreeHead;
import katie.MonitorProof;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.FullTreeHeadProtobufAdapter;
import org.whispersystems.textsecuregcm.util.MonitorProofProtobufAdapter;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;

public record KeyTransparencyMonitorResponse(
    @NotNull
    @JsonSerialize(using = FullTreeHeadProtobufAdapter.Serializer.class)
    @JsonDeserialize(using = FullTreeHeadProtobufAdapter.Deserializer.class)
    @Schema(description = """
        The key transparency log's tree head along with a consistency proof and possibly an auditor-signed tree head
        """)
    FullTreeHead fullTreeHead,

    @NotNull
    @JsonSerialize(using = MonitorProofProtobufAdapter.Serializer.class)
    @JsonDeserialize(using = MonitorProofProtobufAdapter.Deserializer.class)
    @Schema(description = "The monitor proof for the aci search key")
    MonitorProof aciMonitorProof,

    @JsonSerialize(contentUsing = MonitorProofProtobufAdapter.Serializer.class)
    @JsonDeserialize(contentUsing = MonitorProofProtobufAdapter.Deserializer.class)
    @Schema(description = "The monitor proof for the e164 search key")
    Optional<MonitorProof> e164MonitorProof,

    @JsonSerialize(contentUsing = MonitorProofProtobufAdapter.Serializer.class)
    @JsonDeserialize(contentUsing = MonitorProofProtobufAdapter.Deserializer.class)
    @Schema(description = "The monitor proof for the username hash search key")
    Optional<MonitorProof> usernameHashMonitorProof,

    @NotNull
    @JsonSerialize(contentUsing = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(contentUsing = ByteArrayAdapter.Deserializing.class)
    @Schema(description = "A list of hashes encoded in standard, unpadded base64 that prove inclusion across all monitor proofs ")
    List<byte[]> inclusionProof
) {}
