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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.calls.routing.TurnCallRouter;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;

@io.swagger.v3.oas.annotations.tags.Tag(name = "Calling")
@Path("/v2/calling")
public class CallRoutingControllerV2 {

  private static final int TURN_INSTANCE_LIMIT = 2;
  private static final Counter INVALID_IP_COUNTER = Metrics.counter(name(CallRoutingControllerV2.class, "invalidIP"));
  private final RateLimiters rateLimiters;
  private final TurnCallRouter turnCallRouter;
  private final TurnTokenGenerator tokenGenerator;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager;

  public CallRoutingControllerV2(
      final RateLimiters rateLimiters,
      final TurnCallRouter turnCallRouter,
      final TurnTokenGenerator tokenGenerator,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager
  ) {
    this.rateLimiters = rateLimiters;
    this.turnCallRouter = turnCallRouter;
    this.tokenGenerator = tokenGenerator;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
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
      final @ReadOnly @Auth AuthenticatedDevice auth,
      @Context ContainerRequestContext requestContext
  ) throws RateLimitExceededException, IOException {
    UUID aci = auth.getAccount().getUuid();
    rateLimiters.getCallEndpointLimiter().validate(aci);

    List<TurnToken> tokens = new ArrayList<>();
    if (experimentEnrollmentManager.isEnrolled(auth.getAccount().getNumber(), aci, "cloudflareTurn")) {
      tokens.add(cloudflareTurnCredentialsManager.retrieveFromCloudflare());
    }

    Optional<InetAddress> address = Optional.empty();
    try {
      final String remoteAddress = (String) requestContext.getProperty(
          RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);
      address = Optional.of(InetAddress.getByName(remoteAddress));
    } catch (UnknownHostException e) {
      INVALID_IP_COUNTER.increment();
    }

    TurnServerOptions options = turnCallRouter.getRoutingFor(aci, address, TURN_INSTANCE_LIMIT);
    tokens.add(tokenGenerator.generateWithTurnServerOptions(options));

    return new GetCallingRelaysResponse(tokens);
  }

  public record GetCallingRelaysResponse(
      List<TurnToken> relays
  ) {
  }
}
