package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.storage.Account;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.security.InvalidKeyException;

import io.dropwizard.auth.Auth;

@Path("/v1/certificate")
public class CertificateController {

  private final CertificateGenerator certificateGenerator;

  public CertificateController(CertificateGenerator certificateGenerator) {
    this.certificateGenerator = certificateGenerator;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delivery")
  public DeliveryCertificate getDeliveryCertificate(@Auth Account account) throws IOException, InvalidKeyException {
    if (!account.getAuthenticatedDevice().isPresent()) throw new AssertionError();
    return new DeliveryCertificate(certificateGenerator.createFor(account, account.getAuthenticatedDevice().get()));
  }

}
