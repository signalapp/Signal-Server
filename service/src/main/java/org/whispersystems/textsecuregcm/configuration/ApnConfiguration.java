/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;


public record ApnConfiguration(@NotBlank String teamId,
                               @NotBlank String keyId,
                               @NotNull SecretString signingKey,
                               @NotBlank String bundleId,
                               boolean sandbox) {
}
