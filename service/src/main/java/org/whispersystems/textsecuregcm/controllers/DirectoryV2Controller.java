/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Clock;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
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

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAuthToken(final @Auth AuthenticatedAccount auth) {
    final UUID uuid = auth.getAccount().getUuid();
    final ExternalServiceCredentials credentials = directoryServiceTokenGenerator.generateForUuid(uuid);
    return Response.ok().entity(credentials).build();
  }
}
