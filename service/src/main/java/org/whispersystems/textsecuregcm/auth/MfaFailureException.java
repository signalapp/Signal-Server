/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.webauthn.AuthenticationCeremonyParameters;
import org.whispersystems.textsecuregcm.util.NoStackTraceException;

public class MfaFailureException extends NoStackTraceException {

  private final @Nullable AuthenticationCeremonyParameters webAuthnParameters;
  private final boolean hasTotp;

  public MfaFailureException(@Nullable AuthenticationCeremonyParameters webAuthnParameters, boolean hasTotpKey) {
    this.webAuthnParameters = webAuthnParameters;
    this.hasTotp = hasTotpKey;
  }

  public @Nullable AuthenticationCeremonyParameters getWebAuthnParameters() {
    return webAuthnParameters;
  }

  public boolean hasTotpKey() {
    return hasTotp;
  }
}
