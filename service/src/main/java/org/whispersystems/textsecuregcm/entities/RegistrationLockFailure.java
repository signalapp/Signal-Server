/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

@Schema(description = """
    Information about the current Registration lock and SVR credentials. With a correct PIN, the credentials can
    be used to recover the secret used to derive the registration lock password.
    """)
public record RegistrationLockFailure(
    @Schema(description = "Time remaining in milliseconds before the existing registration lock expires")
    long timeRemaining,
    @Schema(description = "Credentials that can be used with SVR2")
    @Nullable
    ExternalServiceCredentials svr2Credentials) {
}
