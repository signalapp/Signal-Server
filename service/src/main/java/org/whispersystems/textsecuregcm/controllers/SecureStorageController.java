/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;

@Path("/v1/storage")
@Tag(name = "Secure Storage")
public class SecureStorageController {

  private final ExternalServiceCredentialsGenerator storageServiceCredentialsGenerator;

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureStorageServiceConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }

  public SecureStorageController(ExternalServiceCredentialsGenerator storageServiceCredentialsGenerator) {
    this.storageServiceCredentialsGenerator = storageServiceCredentialsGenerator;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for Storage Service",
      description = """
          Generate Storage Service credentials. Generated credentials have an expiration time of 24 hours\s
          (however, the TTL is fully controlled by the server and may change even for already generated credentials).
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  public ExternalServiceCredentials getAuth(@Auth AuthenticatedDevice auth) {
    return storageServiceCredentialsGenerator.generateForUuid(auth.accountIdentifier());
  }
}
