/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;

public record Redis(@NotNull @Valid RedisClusterConfiguration rateLimiters) {
}
