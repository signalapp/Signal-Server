/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.MfaFailureException;
import org.whispersystems.textsecuregcm.auth.webauthn.WebAuthnAuthenticationParameters;

public class MfaFailureExceptionMapper implements ExceptionMapper<MfaFailureException>  {

  @Override
  public Response toResponse(final MfaFailureException exception) {
    return Response.status(441)
        .entity(new MfaFailureResponse(
            exception.getWebAuthnParameters() == null
            ? null
            : new WebAuthnAuthenticationParameters(exception.getWebAuthnParameters().challenge(),
                exception.getWebAuthnParameters().timeout().toSeconds(),
                exception.getWebAuthnParameters().allowedCredentialIds()),
            exception.hasTotpKey()))
        .build();
  }

  @Schema(description = """
      Information about the account's MFA status.
      """)
  public record MfaFailureResponse(
      @Schema(description = "If the account has a WebAuthn credential, this field contains parameters for an authentication ceremony.")
      @Nullable WebAuthnAuthenticationParameters webAuthnParameters,
      @Schema(description = "Whether the account has a TOTP key configured")
      boolean hasTotpKey) {
  }

}
