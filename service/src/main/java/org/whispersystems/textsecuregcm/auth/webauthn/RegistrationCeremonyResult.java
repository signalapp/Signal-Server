/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import com.webauthn4j.data.attestation.authenticator.AttestedCredentialData;
import com.webauthn4j.data.client.CollectedClientData;

/// Results of a registration ceremony for persisting.
public record RegistrationCeremonyResult(
    AttestedCredentialData attestedCredentialData,
    CollectedClientData collectedClientData,
    long signCount
) {
}
