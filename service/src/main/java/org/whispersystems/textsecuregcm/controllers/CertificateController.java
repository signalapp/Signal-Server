/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import java.security.InvalidKeyException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/certificate")
public class CertificateController {

  private final CertificateGenerator   certificateGenerator;
  private final ServerZkAuthOperations serverZkAuthOperations;

  private static final String GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME = name(CertificateGenerator.class, "generateCertificate");
  private static final String INCLUDE_E164_TAG_NAME = "includeE164";

  public CertificateController(CertificateGenerator certificateGenerator, ServerZkAuthOperations serverZkAuthOperations) {
    this.certificateGenerator   = certificateGenerator;
    this.serverZkAuthOperations = serverZkAuthOperations;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delivery")
  public DeliveryCertificate getDeliveryCertificate(@Auth AuthenticatedAccount auth,
      @QueryParam("includeE164") Optional<Boolean> maybeIncludeE164)
      throws InvalidKeyException {
    if (Util.isEmpty(auth.getAccount().getIdentityKey())) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    final boolean includeE164 = maybeIncludeE164.orElse(true);

    Metrics.counter(GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME, INCLUDE_E164_TAG_NAME, String.valueOf(includeE164))
        .increment();

    return new DeliveryCertificate(
        certificateGenerator.createFor(auth.getAccount(), auth.getAuthenticatedDevice(), includeE164));
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/group/{startRedemptionTime}/{endRedemptionTime}")
  public GroupCredentials getAuthenticationCredentials(@Auth AuthenticatedAccount auth,
      @PathParam("startRedemptionTime") int startRedemptionTime,
      @PathParam("endRedemptionTime") int endRedemptionTime,
      @QueryParam("identity") Optional<String> identityType) {
    if (startRedemptionTime > endRedemptionTime) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }
    if (endRedemptionTime > Util.currentDaysSinceEpoch() + 7) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }
    if (startRedemptionTime < Util.currentDaysSinceEpoch()) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    List<GroupCredentials.GroupCredential> credentials = new LinkedList<>();

    final UUID identifier = identityType.map(String::toLowerCase).orElse("aci").equals("pni") ?
        auth.getAccount().getPhoneNumberIdentifier() :
        auth.getAccount().getUuid();

    for (int i = startRedemptionTime; i <= endRedemptionTime; i++) {
      credentials.add(new GroupCredentials.GroupCredential(
          serverZkAuthOperations.issueAuthCredential(identifier, i).serialize(),
          i));
    }

    return new GroupCredentials(credentials);
  }

}
