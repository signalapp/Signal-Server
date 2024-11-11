/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;


public record ApnConfiguration(@NotNull SecretString teamId,
                               @NotNull SecretString keyId,
                               @NotNull SecretString signingKey,
                               @NotBlank String bundleId,
                               boolean sandbox) {
}
