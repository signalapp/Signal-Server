/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration.dynamic;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import java.math.BigDecimal;

/// @param rejectConnectionRatio The proportion of connection attempts that should be immediately closed with a GOAWAY
public record DynamicOmnibusConfiguration(@DecimalMin("0.0") @DecimalMax("1.0") BigDecimal rejectConnectionRatio) {
}
