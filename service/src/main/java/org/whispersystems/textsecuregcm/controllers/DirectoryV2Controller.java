/*
 * Copyright 2013 Signal Messenger, LLC
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
import org.whispersystems.textsecuregcm.configuration.DirectoryV2ClientConfiguration;

@Path("/v2/directory")
@Tag(name = "Directory")
public class DirectoryV2Controller {

  private final ExternalServiceCredentialsGenerator directoryServiceTokenGenerator;

  @VisibleForTesting
  public static ExternalServiceCredentialsGenerator credentialsGenerator(final DirectoryV2ClientConfiguration cfg,
                                                                        final Clock clock) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret())
        .prependUsername(false)
        .withClock(clock)
        .build();
  }

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final DirectoryV2ClientConfiguration cfg) {
    return credentialsGenerator(cfg, Clock.systemUTC());
  }

  public DirectoryV2Controller(final ExternalServiceCredentialsGenerator userTokenGenerator) {
    this.directoryServiceTokenGenerator = userTokenGenerator;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for Contact Discovery Service",
      description = """
          Generate Contact Discovery Service credentials. Generated credentials have an expiration time of 24 hours\s
          (however, the TTL is fully controlled by the server and may change even for already generated credentials).
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  public ExternalServiceCredentials getAuthToken(final @Auth AuthenticatedDevice auth) {
    return directoryServiceTokenGenerator.generateForUuid(auth.accountIdentifier());
  }
}
