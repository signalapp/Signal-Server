/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.ArtServiceConfiguration;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/art")
@Tag(name = "Art")
public class ArtController {
  private final ExternalServiceCredentialsGenerator artServiceCredentialsGenerator;
  private final RateLimiters rateLimiters;

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final ArtServiceConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userAuthenticationTokenUserIdSecret())
        .prependUsername(false)
        .truncateSignature(false)
        .build();
  }

  public ArtController(final RateLimiters rateLimiters,
                       final ExternalServiceCredentialsGenerator artServiceCredentialsGenerator) {
    this.artServiceCredentialsGenerator = artServiceCredentialsGenerator;
    this.rateLimiters = rateLimiters;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(final @ReadOnly @Auth AuthenticatedDevice auth)
    throws RateLimitExceededException {
    final UUID uuid = auth.getAccount().getUuid();
    rateLimiters.forDescriptor(RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS).validate(uuid);
    return artServiceCredentialsGenerator.generateForUuid(uuid);
  }
}
