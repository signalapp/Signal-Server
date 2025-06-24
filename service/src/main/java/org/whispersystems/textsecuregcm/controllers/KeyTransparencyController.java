/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.Auth;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Optional;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.keytransparency.client.AciMonitorRequest;
import org.signal.keytransparency.client.E164MonitorRequest;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.UsernameHashMonitorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyDistinguishedKeyResponse;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyMonitorRequest;
import org.whispersystems.textsecuregcm.entities.KeyTransparencyMonitorResponse;
import org.whispersystems.textsecuregcm.entities.KeyTransparencySearchRequest;
import org.whispersystems.textsecuregcm.entities.KeyTransparencySearchResponse;
import org.whispersystems.textsecuregcm.keytransparency.KeyTransparencyServiceClient;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

@Path("/v1/key-transparency")
@Tag(name = "KeyTransparency")
public class KeyTransparencyController {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyTransparencyController.class);
  private final KeyTransparencyServiceClient keyTransparencyServiceClient;

  public KeyTransparencyController(
      final KeyTransparencyServiceClient keyTransparencyServiceClient) {
    this.keyTransparencyServiceClient = keyTransparencyServiceClient;
  }

  @Operation(
      summary = "Search for the given identifiers in the key transparency log",
      description = """
          Returns a response if the ACI exists in the transparency log and its mapped value matches the provided
          ACI identity key.

          The username hash search response field is populated if it is found in the log and its mapped value matches
          the provided ACI. The E164 search response is populated similarly, with some additional requirements:
          - The account associated with the provided ACI must be discoverable by phone number.
          - The provided unidentified access key must match the one on the account.

          Enforced unauthenticated endpoint.
          """
  )
  @ApiResponse(responseCode = "200", description = "The ACI was found and its mapped value matched the provided ACI identity key", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid request. See response for any available details.")
  @ApiResponse(responseCode = "403", description = "The ACI was found but its mapped value did not match the provided ACI identity key")
  @ApiResponse(responseCode = "404", description = "The ACI was not found in the log")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @ApiResponse(responseCode = "429", description = "Rate-limited")
  @POST
  @Path("/search")
  @RateLimitedByIp(RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public KeyTransparencySearchResponse search(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final KeyTransparencySearchRequest request) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      final Optional<E164SearchRequest> maybeE164SearchRequest =
          request.e164().flatMap(e164 -> request.unidentifiedAccessKey().map(uak ->
              E164SearchRequest.newBuilder()
                  .setE164(e164)
                  .setUnidentifiedAccessKey(ByteString.copyFrom(request.unidentifiedAccessKey().get()))
                  .build()
          ));

      return new KeyTransparencySearchResponse(
          keyTransparencyServiceClient.search(
              ByteString.copyFrom(request.aci().toCompactByteArray()),
              ByteString.copyFrom(request.aciIdentityKey().serialize()),
              request.usernameHash().map(ByteString::copyFrom),
              maybeE164SearchRequest,
              request.lastTreeHeadSize(),
              request.distinguishedTreeHeadSize())
          .toByteArray());
    } catch (final StatusRuntimeException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  @Operation(
      summary = "Monitor the given identifiers in the key transparency log",
      description = """
          Return proofs proving that the log tree has been constructed correctly in later entries for each of the given
          identifiers. Enforced unauthenticated endpoint.
          """
  )
  @ApiResponse(responseCode = "200", description = "All identifiers exist in the log", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid request. See response for any available details.")
  @ApiResponse(responseCode = "403", description = "One or more of the provided commitment indexes did not match")
  @ApiResponse(responseCode = "404", description = "At least one identifier was not found")
  @ApiResponse(responseCode = "429", description = "Rate-limited")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @POST
  @Path("/monitor")
  @RateLimitedByIp(RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public KeyTransparencyMonitorResponse monitor(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final KeyTransparencyMonitorRequest request) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      final AciMonitorRequest aciMonitorRequest = AciMonitorRequest.newBuilder()
          .setAci(ByteString.copyFrom(request.aci().value().toCompactByteArray()))
          .setEntryPosition(request.aci().entryPosition())
          .setCommitmentIndex(ByteString.copyFrom(request.aci().commitmentIndex()))
          .build();

      final Optional<UsernameHashMonitorRequest> usernameHashMonitorRequest = request.usernameHash().map(usernameHash ->
          UsernameHashMonitorRequest.newBuilder()
              .setUsernameHash(ByteString.copyFrom(usernameHash.value()))
              .setEntryPosition(usernameHash.entryPosition())
              .setCommitmentIndex(ByteString.copyFrom(usernameHash.commitmentIndex()))
              .build());

      final Optional<E164MonitorRequest> e164MonitorRequest = request.e164().map(e164 ->
          E164MonitorRequest.newBuilder()
              .setE164(e164.value())
              .setEntryPosition(e164.entryPosition())
              .setCommitmentIndex(ByteString.copyFrom(e164.commitmentIndex()))
              .build());

      return new KeyTransparencyMonitorResponse(keyTransparencyServiceClient.monitor(
          aciMonitorRequest,
          usernameHashMonitorRequest,
          e164MonitorRequest,
          request.lastNonDistinguishedTreeHeadSize(),
          request.lastDistinguishedTreeHeadSize())
          .toByteArray());
    } catch (final StatusRuntimeException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  @Operation(
      summary = "Get the current value of the distinguished key",
      description = """
          The response contains the distinguished tree head to prove consistency
          against for future calls to `/search`, `/monitor`, and `/distinguished`.
          Enforced unauthenticated endpoint.
          """
  )
  @ApiResponse(responseCode = "200", description = "The `distinguished` search key exists in the log", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid request. See response for any available details.")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @ApiResponse(responseCode = "429", description = "Rate-limited")
  @GET
  @Path("/distinguished")
  @RateLimitedByIp(RateLimiters.For.KEY_TRANSPARENCY_DISTINGUISHED_PER_IP)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public KeyTransparencyDistinguishedKeyResponse getDistinguishedKey(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,

      @Parameter(description = "The distinguished tree head size returned by a previously verified call")
      @QueryParam("lastTreeHeadSize") @Valid final Optional<@Positive Long> lastTreeHeadSize) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      return new KeyTransparencyDistinguishedKeyResponse(
          keyTransparencyServiceClient.getDistinguishedKey(lastTreeHeadSize)
          .toByteArray());
    } catch (final StatusRuntimeException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  private void handleKeyTransparencyServiceError(final StatusRuntimeException exception) {
    final Status.Code code = exception.getStatus().getCode();
    final String description = exception.getStatus().getDescription();
    switch (code) {
      case NOT_FOUND -> throw new NotFoundException(description);
      case PERMISSION_DENIED -> throw new ForbiddenException(description);
      case INVALID_ARGUMENT -> throw new WebApplicationException(description, 422);
      default -> {
        LOGGER.error("Unexpected error calling key transparency service", exception);
        throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, exception);
      }
    }
  }

  private void requireNotAuthenticated(final Optional<AuthenticatedDevice> authenticatedAccount) {
    if (authenticatedAccount.isPresent()) {
      throw new BadRequestException("Endpoint requires unauthenticated access");
    }
  }

}
