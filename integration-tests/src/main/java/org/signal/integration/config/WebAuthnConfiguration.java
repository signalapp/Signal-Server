/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

import jakarta.validation.constraints.NotBlank;

public record WebAuthnConfiguration(
    @NotBlank String relyingPartyId,
    @NotBlank String origin) {
}
