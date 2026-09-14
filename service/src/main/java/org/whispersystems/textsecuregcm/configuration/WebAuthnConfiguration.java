/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record WebAuthnConfiguration(
  @NotBlank String relyingPartyId,
  @NotBlank String origin,
  @NotNull Duration challengeTtl,
  @NotNull @ExactlySize(32) SecretBytes userHandleBlindingSecret) {
}
