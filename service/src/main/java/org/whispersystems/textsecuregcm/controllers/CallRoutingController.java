package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.calls.routing.TurnCallRouter;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

@Path("/v1/calling")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Calling")
public class CallRoutingController {

  private static final int TURN_INSTANCE_LIMIT = 6;
  private static final Counter INVALID_IP_COUNTER = Metrics.counter(name(CallRoutingController.class, "invalidIP"));
  private static final Logger log = LoggerFactory.getLogger(CallRoutingController.class);
  private final RateLimiters rateLimiters;
  private final TurnCallRouter turnCallRouter;
  private final TurnTokenGenerator tokenGenerator;

  public CallRoutingController(
      final RateLimiters rateLimiters,
      final TurnCallRouter turnCallRouter,
      final TurnTokenGenerator tokenGenerator
  ) {
    this.rateLimiters = rateLimiters;
    this.turnCallRouter = turnCallRouter;
    this.tokenGenerator = tokenGenerator;
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
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public TurnToken getCallingRelays(
      final @ReadOnly @Auth AuthenticatedAccount auth,
      @Context ContainerRequestContext requestContext
  ) throws RateLimitExceededException {
    UUID aci = auth.getAccount().getUuid();
    rateLimiters.getCallEndpointLimiter().validate(aci);

    Optional<InetAddress> address = Optional.empty();
    try {
      final String remoteAddress = (String) requestContext.getProperty(
          RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);
      address = Optional.of(InetAddress.getByName(remoteAddress));
    } catch (UnknownHostException e) {
      INVALID_IP_COUNTER.increment();
    }

    TurnServerOptions options = turnCallRouter.getRoutingFor(aci, address, TURN_INSTANCE_LIMIT);
    return tokenGenerator.generateWithTurnServerOptions(options);
  }
}
