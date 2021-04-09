/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.auth.Auth;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.storage.Account;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

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
  public Response getAuthToken(@Auth Account account) {
    return Response.ok().entity(directoryServiceTokenGenerator.generateFor(account.getNumber())).build();
  }

  @PUT
  @Path("/feedback-v3/{status}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setFeedback(@Auth Account account) {
    return Response.ok().build();
  }


  @Timed
  @GET
  @Path("/{token}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTokenPresence(@Auth Account account) {
    return Response.status(429).build();
  }

  @Timed
  @PUT
  @Path("/tokens")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getContactIntersection(@Auth Account account) {
    return Response.status(429).build();
  }
}
