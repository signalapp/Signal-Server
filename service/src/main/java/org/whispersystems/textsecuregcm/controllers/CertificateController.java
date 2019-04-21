package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.security.InvalidKeyException;

import io.dropwizard.auth.Auth;

@Path("/v1/certificate")
public class CertificateController {

  private final Logger logger = LoggerFactory.getLogger(CertificateController.class);

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

    if (Util.isEmpty(account.getIdentityKey())) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    return new DeliveryCertificate(certificateGenerator.createFor(account, account.getAuthenticatedDevice().get()));
  }

}
