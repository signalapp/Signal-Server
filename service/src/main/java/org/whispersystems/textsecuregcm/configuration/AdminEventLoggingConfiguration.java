/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

public record AdminEventLoggingConfiguration(
    @NotBlank String credentials,
    @NotEmpty String projectId,
    @NotEmpty String logName) {
}
