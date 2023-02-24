/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery2Configuration;

@Path("/v2/backup")
@Tag(name = "Secure Value Recovery")
public class SecureValueRecovery2Controller {

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecovery2Configuration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret())
        .build();
  }

  private final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator;

  public SecureValueRecovery2Controller(final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator) {
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for SVR2",
      description = """
          Generate SVR2 service credentials. Generated credentials have an expiration time of 30 days 
          (however, the TTL is fully controlled by the server side and may change even for already generated credentials). 
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public ExternalServiceCredentials getAuth(@Auth final AuthenticatedAccount auth) {
    return backupServiceCredentialGenerator.generateFor(auth.getAccount().getUuid().toString());
  }
}
