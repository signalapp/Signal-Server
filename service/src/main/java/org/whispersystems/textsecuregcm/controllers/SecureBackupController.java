package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.storage.Account;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.dropwizard.auth.Auth;

@Path("/v1/backup")
public class SecureBackupController {

  private final ExternalServiceCredentialGenerator backupServiceCredentialGenerator;

  public SecureBackupController(ExternalServiceCredentialGenerator backupServiceCredentialGenerator) {
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(@Auth Account account) {
    return backupServiceCredentialGenerator.generateFor(account.getUuid().toString());
  }
}
