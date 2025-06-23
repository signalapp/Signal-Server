/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;

/**
 * The provisioning controller facilitates transmission of provisioning messages from the primary device associated with
 * an existing Signal account to a new device. To send a provisioning message, a primary device generally scans a QR
 * code displayed by the new device that contains the device's "provisioning address" and a public key. The primary
 * device then encrypts a use-case-specific provisioning message and posts it to
 * {@link #sendProvisioningMessage(AuthenticatedDevice, String, ProvisioningMessage, String)}, at which point the server
 * delivers the message to the new device via an open provisioning WebSocket.
 *
 * @see org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener
 */
@Path("/v1/provisioning")
@Tag(name = "Provisioning")
public class ProvisioningController {

  private final RateLimiters rateLimiters;
  private final ProvisioningManager provisioningManager;

  @VisibleForTesting
  private static final long MAX_MESSAGE_SIZE = DataSize.kibibytes(256).toBytes();

  private static final String REJECT_OVERSIZE_MESSAGE_COUNTER =
      name(ProvisioningController.class, "rejectOversizeMessage");

  public ProvisioningController(RateLimiters rateLimiters, ProvisioningManager provisioningManager) {
    this.rateLimiters = rateLimiters;
    this.provisioningManager = provisioningManager;
  }

  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Send a provisioning message to a new device",
      description = """
          Send a provisioning message from an authenticated device to a device that (presumably) is not yet associated
          with a Signal account.
          """)
  @ApiResponse(responseCode="204", description="The provisioning message was delivered to the given provisioning address")
  @ApiResponse(responseCode="400", description="The provisioning message was too large")
  @ApiResponse(responseCode="404", description="No device with the given provisioning address was connected at the time of the request")
  public void sendProvisioningMessage(@Auth final AuthenticatedDevice auth,

      @Parameter(description = "The temporary provisioning address to which to send a provisioning message")
      @PathParam("destination") final String provisioningAddress,

      @Parameter(description = "The provisioning message to send to the given provisioning address")
      @NotNull @Valid final ProvisioningMessage message,

      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent)
      throws RateLimitExceededException {

    if (message.body().length() > MAX_MESSAGE_SIZE) {
      Metrics.counter(REJECT_OVERSIZE_MESSAGE_COUNTER, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    rateLimiters.getMessagesLimiter().validate(auth.accountIdentifier());

    final boolean subscriberPresent =
        provisioningManager.sendProvisioningMessage(provisioningAddress, Base64.getMimeDecoder().decode(message.body()));

    if (!subscriberPresent) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }
  }
}
