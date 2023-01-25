/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.codec.DecoderException;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;

@Path("/v1/backup")
public class SecureBackupController {

  private final ExternalServiceCredentialsGenerator backupServiceCredentialsGenerator;

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureBackupServiceConfiguration cfg)
      throws DecoderException {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.getUserAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }

  public SecureBackupController(ExternalServiceCredentialsGenerator backupServiceCredentialsGenerator) {
    this.backupServiceCredentialsGenerator = backupServiceCredentialsGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(@Auth AuthenticatedAccount auth) {
    return backupServiceCredentialsGenerator.generateForUuid(auth.getAccount().getUuid());
  }
}
