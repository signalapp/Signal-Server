/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery2Configuration;

@Path("/v2/backup")
public class SecureValueRecovery2Controller {

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecovery2Configuration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret())
        .build();
  }

  private final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator;

  public SecureValueRecovery2Controller(ExternalServiceCredentialsGenerator backupServiceCredentialGenerator) {
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(@Auth AuthenticatedAccount auth) {
    return backupServiceCredentialGenerator.generateFor(auth.getAccount().getUuid().toString());
  }
}
