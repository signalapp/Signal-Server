/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import java.util.List;

/// Server-generated parameters for a freshly-initiated registration ceremony.
public record RegistrationCeremonyParameters(
    byte[] userHandle,
    List<Long> allowedAlgorithms,
    List<byte[]> excludedCredentialIds) {
}
