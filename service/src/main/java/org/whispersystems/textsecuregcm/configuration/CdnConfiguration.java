/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public record CdnConfiguration(@NotNull @Valid StaticAwsCredentialsFactory credentials,
                               @NotBlank String bucket,
                               @NotBlank String region) {
}
