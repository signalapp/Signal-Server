/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.websocket.session.WebSocketSession;
import org.whispersystems.websocket.session.WebSocketSessionContext;


@Path("/v1/keepalive")
@Tag(name = "Keep Alive")
public class KeepAliveController {

  private final Logger logger = LoggerFactory.getLogger(KeepAliveController.class);

  private final WebSocketConnectionEventManager webSocketConnectionEventManager;

  private static final String CLOSED_CONNECTION_AGE_DISTRIBUTION_NAME = name(KeepAliveController.class,
      "closedConnectionAge");


  public KeepAliveController(final WebSocketConnectionEventManager webSocketConnectionEventManager) {
    this.webSocketConnectionEventManager = webSocketConnectionEventManager;
  }

  @GET
  public Response getKeepAlive(@Auth Optional<AuthenticatedDevice> maybeAuth,
      @WebSocketSession WebSocketSessionContext context) {

    maybeAuth.ifPresent(auth -> {
      if (!webSocketConnectionEventManager.isLocallyPresent(auth.accountIdentifier(), auth.deviceId())) {

        final Duration age = Duration.between(context.getClient().getCreated(), Instant.now());

        logger.debug("***** No local subscription found for {}::{}; age = {}ms, User-Agent = {}",
            auth.accountIdentifier(), auth.deviceId(), age.toMillis(),
            context.getClient().getUserAgent());

        context.getClient().close(1000, "OK");

        Timer.builder(CLOSED_CONNECTION_AGE_DISTRIBUTION_NAME)
            .tags(Tags.of(UserAgentTagUtil.getPlatformTag(context.getClient().getUserAgent())))
            .publishPercentileHistogram(true)
            .register(Metrics.globalRegistry)
            .record(age);
      }
    });

    return Response.ok().build();
  }

  @GET
  @Path("/provisioning")
  public Response getProvisioningKeepAlive() {
    return Response.ok().build();
  }

}
