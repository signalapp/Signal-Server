/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record WebAuthnConfiguration(
  @NotBlank String relyingPartyId,
  @NotEmpty List<@NotBlank String> origins,
  @NotNull Duration challengeTtl,
  @NotNull @ExactlySize(32) SecretBytes userHandleBlindingSecret) {
}
