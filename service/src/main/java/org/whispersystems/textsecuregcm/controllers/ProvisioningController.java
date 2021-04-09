/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.websocket.InvalidWebsocketAddressException;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import io.dropwizard.auth.Auth;

@Path("/v1/provisioning")
public class ProvisioningController {

  private final RateLimiters        rateLimiters;
  private final ProvisioningManager provisioningManager;

  public ProvisioningController(RateLimiters rateLimiters, ProvisioningManager provisioningManager) {
    this.rateLimiters        = rateLimiters;
    this.provisioningManager = provisioningManager;
  }

  @Timed
  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void sendProvisioningMessage(@Auth                     Account source,
                                      @PathParam("destination") String destinationName,
                                      @Valid                    ProvisioningMessage message)
      throws RateLimitExceededException, InvalidWebsocketAddressException, IOException
  {
    rateLimiters.getMessagesLimiter().validate(source.getNumber());

    if (!provisioningManager.sendProvisioningMessage(new ProvisioningAddress(destinationName, 0),
                                                     Base64.decode(message.getBody())))
    {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }
  }
}
