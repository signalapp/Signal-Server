/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({
    @JsonSubTypes.Type(value = RemoteAttachment.class, name = "success"),
    @JsonSubTypes.Type(value = RemoteAttachmentError.class, name = "error"),
})
@Schema(description = """
        The location of the transfer archive if the archive was successfully uploaded, otherwise a error indicating that
         the upload has failed and the destination device should stop waiting
        """, oneOf = {RemoteAttachmentError.class, RemoteAttachment.class})
public sealed interface TransferArchiveResult permits RemoteAttachment, RemoteAttachmentError {}
