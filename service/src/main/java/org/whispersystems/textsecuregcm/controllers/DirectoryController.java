/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;

@Path("/v1/directory")
public class DirectoryController {

  private final ExternalServiceCredentialGenerator directoryServiceTokenGenerator;

  public DirectoryController(ExternalServiceCredentialGenerator userTokenGenerator) {
    this.directoryServiceTokenGenerator = userTokenGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAuthToken(@Auth AuthenticatedAccount auth) {
    return Response.ok().entity(directoryServiceTokenGenerator.generateFor(auth.getAccount().getNumber())).build();
  }

  @PUT
  @Path("/feedback-v3/{status}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setFeedback(@Auth AuthenticatedAccount auth) {
    return Response.ok().build();
  }


  @Timed
  @GET
  @Path("/{token}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTokenPresence(@Auth AuthenticatedAccount auth) {
    return Response.status(429).build();
  }

  @Timed
  @PUT
  @Path("/tokens")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getContactIntersection(@Auth AuthenticatedAccount auth) {
    return Response.status(429).build();
  }
}
