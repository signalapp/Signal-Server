/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.websocket.session.WebSocketSession;
import org.whispersystems.websocket.session.WebSocketSessionContext;


@Path("/v1/keepalive")
public class KeepAliveController {

  private final Logger logger = LoggerFactory.getLogger(KeepAliveController.class);

  private final ClientPresenceManager clientPresenceManager;

  private static final String NO_LOCAL_SUBSCRIPTION_COUNTER_NAME = name(KeepAliveController.class, "noLocalSubscription");

  public KeepAliveController(final ClientPresenceManager clientPresenceManager) {
    this.clientPresenceManager = clientPresenceManager;
  }

  @Timed
  @GET
  public Response getKeepAlive(@Auth AuthenticatedAccount auth,
      @WebSocketSession WebSocketSessionContext context) {
    if (auth != null) {
      if (!clientPresenceManager.isLocallyPresent(auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId())) {
        logger.debug("***** No local subscription found for {}::{}; age = {}ms, User-Agent = {}",
            auth.getAccount().getUuid(), auth.getAuthenticatedDevice().getId(),
            System.currentTimeMillis() - context.getClient().getCreatedTimestamp(),
            context.getClient().getUserAgent());

        context.getClient().close(1000, "OK");

        Metrics.counter(NO_LOCAL_SUBSCRIPTION_COUNTER_NAME,
            Tags.of(UserAgentTagUtil.getPlatformTag(context.getClient().getUserAgent())))
            .increment();
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
