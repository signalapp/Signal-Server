/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Schema(description = "Indicates an attachment failed to upload")
public record RemoteAttachmentError(
    @Schema(description = "The type of error encountered")
    @Valid @NotNull ErrorType error)
    implements TransferArchiveResult {

  public enum ErrorType {
    RELINK_REQUESTED,
    CONTINUE_WITHOUT_UPLOAD;
  }
}
