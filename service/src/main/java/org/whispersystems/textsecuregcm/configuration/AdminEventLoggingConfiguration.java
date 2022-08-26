/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public record AdminEventLoggingConfiguration(
    @NotNull @NotEmpty String credentials,
    @NotNull @NotEmpty String projectId,
    @NotNull @NotEmpty String logName) {
}
