package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.signal.zkgroup.auth.ServerZkAuthOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import io.dropwizard.auth.Auth;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/certificate")
public class CertificateController {

  private final Logger logger = LoggerFactory.getLogger(CertificateController.class);

  private final CertificateGenerator   certificateGenerator;
  private final ServerZkAuthOperations serverZkAuthOperations;
  private final boolean                isZkEnabled;

  public CertificateController(CertificateGenerator certificateGenerator, ServerZkAuthOperations serverZkAuthOperations, boolean isZkEnabled) {
    this.certificateGenerator   = certificateGenerator;
    this.serverZkAuthOperations = serverZkAuthOperations;
    this.isZkEnabled            = isZkEnabled;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delivery")
  public DeliveryCertificate getDeliveryCertificate(@Auth Account account,
                                                    @QueryParam("includeUuid") Optional<Boolean> includeUuid)
      throws IOException, InvalidKeyException
  {
    if (!account.getAuthenticatedDevice().isPresent()) throw new AssertionError();

    if (Util.isEmpty(account.getIdentityKey())) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    return new DeliveryCertificate(certificateGenerator.createFor(account, account.getAuthenticatedDevice().get(), includeUuid.orElse(false)));
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/group/{startRedemptionTime}/{endRedemptionTime}")
  public GroupCredentials getAuthenticationCredentials(@Auth Account account,
                                                       @PathParam("startRedemptionTime") int startRedemptionTime,
                                                       @PathParam("endRedemptionTime") int endRedemptionTime)
  {
    if (!isZkEnabled)                                         throw new WebApplicationException(Response.Status.NOT_FOUND);
    if (startRedemptionTime > endRedemptionTime)              throw new WebApplicationException(Response.Status.BAD_REQUEST);
    if (endRedemptionTime > Util.currentDaysSinceEpoch() + 7) throw new WebApplicationException(Response.Status.BAD_REQUEST);
    if (startRedemptionTime < Util.currentDaysSinceEpoch())   throw new WebApplicationException(Response.Status.BAD_REQUEST);

    List<GroupCredentials.GroupCredential> credentials = new LinkedList<>();

    for (int i=startRedemptionTime;i<=endRedemptionTime;i++) {
      credentials.add(new GroupCredentials.GroupCredential(serverZkAuthOperations.issueAuthCredential(account.getUuid(), i)
                                                                                 .serialize(),
                                                           i));
    }

    return new GroupCredentials(credentials);
  }

}
