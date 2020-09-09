package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.websocket.session.WebSocketSession;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;


@Path("/v1/keepalive")
public class KeepAliveController {

  private final Logger logger = LoggerFactory.getLogger(KeepAliveController.class);

  private final ClientPresenceManager clientPresenceManager;

  public KeepAliveController(final ClientPresenceManager clientPresenceManager) {
    this.clientPresenceManager = clientPresenceManager;
  }

  @Timed
  @GET
  public Response getKeepAlive(@Auth             Account account,
                               @WebSocketSession WebSocketSessionContext context)
  {
    if (account != null) {
      if (!clientPresenceManager.isLocallyPresent(account.getUuid(), account.getAuthenticatedDevice().get().getId())) {
        logger.warn("***** No local subscription found for {}::{}", account.getUuid(), account.getAuthenticatedDevice().get().getId());
        context.getClient().close(1000, "OK");
      }
    }

    return Response.ok().build();
  }

  @Timed
  @GET
  @Path("/provisioning")
  public Response getProvisioningKeepAlive() {
    return Response.ok().build();
  }

}
