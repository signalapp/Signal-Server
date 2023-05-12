/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

@Schema(description = "A token provided to the client via a push payload")

public record RegistrationLockFailure(
    @Schema(description = "Time remaining in milliseconds before the existing registration lock expires")
    long timeRemaining,
    @Schema(description = "Credentials that can be used with SVR1")
    ExternalServiceCredentials backupCredentials,
    @Schema(description = "Credentials that can be used with SVR2")
    ExternalServiceCredentials svr2Credentials) {
}
