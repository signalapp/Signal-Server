/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.Clock;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;

@Path("/v1/svrb")
@Tag(name = "Secure Value Recovery B")
public class SecureValueRecoveryBController {

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecoveryConfiguration cfg) {
    return credentialsGenerator(cfg, Clock.systemUTC());
  }

  @VisibleForTesting
  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecoveryConfiguration cfg,
      final Clock clock) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret().value())
        .prependUsername(false)
        .withDerivedUsernameTruncateLength(16)
        .withClock(clock)
        .build();
  }

  private final ExternalServiceCredentialsGenerator svrbCredentialGenerator;

  public SecureValueRecoveryBController(final ExternalServiceCredentialsGenerator svrbCredentialGenerator) {
    this.svrbCredentialGenerator = svrbCredentialGenerator;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for SVRB",
      description = """
          Generate SVRB service credentials. Generated credentials have an expiration time of 1 day (subject to change)
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public ExternalServiceCredentials getAuth(@Auth final AuthenticatedDevice auth) {
    return svrbCredentialGenerator.generateFor(auth.accountIdentifier().toString());
  }
}
