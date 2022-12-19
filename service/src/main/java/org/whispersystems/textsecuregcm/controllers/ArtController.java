/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

@Path("/v1/art")
public class ArtController {
  private final ExternalServiceCredentialGenerator artServiceCredentialGenerator;
  private final RateLimiters rateLimiters;

  public ArtController(RateLimiters rateLimiters,
      ExternalServiceCredentialGenerator artServiceCredentialGenerator) {
    this.artServiceCredentialGenerator = artServiceCredentialGenerator;
    this.rateLimiters = rateLimiters;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(@Auth AuthenticatedAccount auth)
    throws RateLimitExceededException {
    final UUID uuid = auth.getAccount().getUuid();
    rateLimiters.getArtPackLimiter().validate(uuid);
    return artServiceCredentialGenerator.generateFor(uuid.toString());
  }
}
