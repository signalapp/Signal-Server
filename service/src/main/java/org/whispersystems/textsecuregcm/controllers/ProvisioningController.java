/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Base64;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/provisioning")
@Tag(name = "Provisioning")
public class ProvisioningController {

  private final RateLimiters        rateLimiters;
  private final ProvisioningManager provisioningManager;

  public ProvisioningController(RateLimiters rateLimiters, ProvisioningManager provisioningManager) {
    this.rateLimiters        = rateLimiters;
    this.provisioningManager = provisioningManager;
  }

  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void sendProvisioningMessage(@ReadOnly @Auth AuthenticatedAccount auth,
      @PathParam("destination") String destinationName,
      @NotNull @Valid ProvisioningMessage message)
      throws RateLimitExceededException {

    rateLimiters.getMessagesLimiter().validate(auth.getAccount().getUuid());

    if (!provisioningManager.sendProvisioningMessage(ProvisioningAddress.create(destinationName),
        Base64.getMimeDecoder().decode(message.body()))) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }
  }
}
