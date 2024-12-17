/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import javax.annotation.Nullable;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.registration.VerificationSession;

public record VerificationSessionResponse(
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "A URL-safe ID for the session")
    String id,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Duration in seconds after which next SMS can be requested for this session")
    @Nullable Long nextSms,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Duration in seconds after which next voice call can be requested for this session")
    @Nullable Long nextCall,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Duration in seconds after which the client can submit a verification code for this session")
    @Nullable Long nextVerificationAttempt,

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Whether it is allowed to request a verification code for this session")
    boolean allowedToRequestCode,

    @Schema(description = "A list of requested information that the client needs to submit before requesting code delivery")
    List<VerificationSession.Information> requestedInformation,

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Whether this session is verified")
    boolean verified) {

}
