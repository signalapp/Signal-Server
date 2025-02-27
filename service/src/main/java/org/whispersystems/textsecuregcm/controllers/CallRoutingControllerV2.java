/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;

@io.swagger.v3.oas.annotations.tags.Tag(name = "Calling")
@Path("/v2/calling")
public class CallRoutingControllerV2 {

  private static final Counter CLOUDFLARE_TURN_ERROR_COUNTER = Metrics.counter(name(CallRoutingControllerV2.class, "cloudflareTurnError"));
  private final RateLimiters rateLimiters;
  private final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager;

  public CallRoutingControllerV2(
      final RateLimiters rateLimiters,
      final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager
  ) {
    this.rateLimiters = rateLimiters;
    this.cloudflareTurnCredentialsManager = cloudflareTurnCredentialsManager;
  }

  @GET
  @Path("/relays")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get 1:1 calling relay options for the client",
      description = """
        Get 1:1 relay addresses in IpV4, Ipv6, and URL formats.
        """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with call endpoints.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid get call endpoint request.")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  public GetCallingRelaysResponse getCallingRelays(
      final @ReadOnly @Auth AuthenticatedDevice auth
  ) throws RateLimitExceededException, IOException {
    UUID aci = auth.getAccount().getUuid();
    rateLimiters.getCallEndpointLimiter().validate(aci);

    List<TurnToken> tokens = new ArrayList<>();
    try {
      tokens.add(cloudflareTurnCredentialsManager.retrieveFromCloudflare());
    } catch (Exception e) {
      CallRoutingControllerV2.CLOUDFLARE_TURN_ERROR_COUNTER.increment();
      throw e;
    }

    return new GetCallingRelaysResponse(tokens);
  }

  public record GetCallingRelaysResponse(
      List<TurnToken> relays
  ) {
  }
}
