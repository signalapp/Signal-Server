/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Optional;

public record TransferArchiveUploadedRequest(
    @Min(1)
    @Max(Device.MAXIMUM_DEVICE_ID)
    @Schema(description = "The ID of the device for which the transfer archive has been prepared")
    byte destinationDeviceId,

    @Schema(description = """
      The timestamp, in milliseconds since the epoch, at which the destination device was created.
      Deprecated in favor of `destinationDeviceRegistrationId`.
    """, deprecated = true)
    @Deprecated
    Optional<@Positive Long> destinationDeviceCreated,

    @Schema(description = "The registration ID of the destination device")
    Optional<@Min(0) @Max(Device.MAX_REGISTRATION_ID) Integer> destinationDeviceRegistrationId,

    @NotNull
    @Valid
    @Schema(description = """
          The location of the transfer archive if the archive was successfully uploaded, otherwise a error indicating that
           the upload has failed and the destination device should stop waiting
          """, oneOf = {RemoteAttachment.class, RemoteAttachmentError.class})
    TransferArchiveResult transferArchive) {
  @AssertTrue
  @Schema(hidden = true)
  public boolean isExactlyOneDisambiguatorProvided() {
    return destinationDeviceCreated.isPresent() ^ destinationDeviceRegistrationId.isPresent();
  }
}
