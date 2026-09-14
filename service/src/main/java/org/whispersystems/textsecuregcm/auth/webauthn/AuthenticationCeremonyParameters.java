/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import java.time.Duration;
import java.util.List;

/// Server-generated parameters for a freshly-initiated authentication ceremony.
public record AuthenticationCeremonyParameters(
    byte[] challenge,
    Duration timeout,
    List<byte[]> allowedCredentialIds) {
}
