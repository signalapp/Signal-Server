/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public record GiftConfiguration(
    long level,
    @NotNull Duration expiration,
    @Valid @NotNull Map<@NotEmpty String, @DecimalMin("0.01") @NotNull BigDecimal> currencies,
    @NotEmpty String badge) {
}
