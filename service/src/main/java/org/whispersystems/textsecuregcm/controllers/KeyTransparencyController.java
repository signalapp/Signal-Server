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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.signal.keytransparency.client.MonitorKey;
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
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/key-transparency")
@Tag(name = "KeyTransparency")
public class KeyTransparencyController {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyTransparencyController.class);
  @VisibleForTesting
  static final Duration KEY_TRANSPARENCY_RPC_TIMEOUT = Duration.ofSeconds(15);
  @VisibleForTesting
  static final byte USERNAME_PREFIX = (byte) 'u';
  @VisibleForTesting
  static final byte E164_PREFIX = (byte) 'n';
  @VisibleForTesting
  static final byte ACI_PREFIX = (byte) 'a';
  private final KeyTransparencyServiceClient keyTransparencyServiceClient;

  public KeyTransparencyController(
      final KeyTransparencyServiceClient keyTransparencyServiceClient) {
    this.keyTransparencyServiceClient = keyTransparencyServiceClient;
  }

  @Operation(
      summary = "Search for the given search keys in the key transparency log",
      description = """
          Enforced unauthenticated endpoint. Returns a response if all search keys exist in the key transparency log.
          """
  )
  @ApiResponse(responseCode = "200", description = "All search key lookups were successful", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid request. See response for any available details.")
  @ApiResponse(responseCode = "403", description = "At least one search key lookup to value mapping was invalid")
  @ApiResponse(responseCode = "404", description = "At least one search key lookup did not find the key")
  @ApiResponse(responseCode = "429", description = "Rate-limited")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @POST
  @Path("/search")
  @RateLimitedByIp(RateLimiters.For.KEY_TRANSPARENCY_SEARCH_PER_IP)
  @Produces(MediaType.APPLICATION_JSON)
  public KeyTransparencySearchResponse search(
      @ReadOnly @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final KeyTransparencySearchRequest request) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      final CompletableFuture<byte[]> aciSearchKeyResponseFuture = keyTransparencyServiceClient.search(
          getFullSearchKeyByteString(ACI_PREFIX, request.aci().toCompactByteArray()),
          ByteString.copyFrom(request.aciIdentityKey().serialize()),
          Optional.empty(),
          request.lastTreeHeadSize(),
          request.distinguishedTreeHeadSize(),
          KEY_TRANSPARENCY_RPC_TIMEOUT);

      final CompletableFuture<byte[]> e164SearchKeyResponseFuture = request.e164()
          .map(e164 -> keyTransparencyServiceClient.search(
              getFullSearchKeyByteString(E164_PREFIX, e164.getBytes(StandardCharsets.UTF_8)),
              ByteString.copyFrom(request.aci().toCompactByteArray()),
              Optional.of(ByteString.copyFrom(request.unidentifiedAccessKey().get())),
              request.lastTreeHeadSize(),
              request.distinguishedTreeHeadSize(),
              KEY_TRANSPARENCY_RPC_TIMEOUT))
          .orElse(CompletableFuture.completedFuture(null));

      final CompletableFuture<byte[]> usernameHashSearchKeyResponseFuture = request.usernameHash()
          .map(usernameHash -> keyTransparencyServiceClient.search(
              getFullSearchKeyByteString(USERNAME_PREFIX, request.usernameHash().get()),
              ByteString.copyFrom(request.aci().toCompactByteArray()),
              Optional.empty(),
              request.lastTreeHeadSize(),
              request.distinguishedTreeHeadSize(),
              KEY_TRANSPARENCY_RPC_TIMEOUT))
          .orElse(CompletableFuture.completedFuture(null));

      return CompletableFuture.allOf(aciSearchKeyResponseFuture, e164SearchKeyResponseFuture,
              usernameHashSearchKeyResponseFuture)
          .thenApply(ignored ->
              new KeyTransparencySearchResponse(aciSearchKeyResponseFuture.join(),
                  Optional.ofNullable(e164SearchKeyResponseFuture.join()),
                  Optional.ofNullable(usernameHashSearchKeyResponseFuture.join())))
          .join();
    } catch (final CancellationException exception) {
      LOGGER.error("Unexpected cancellation from key transparency service", exception);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, exception);
    } catch (final CompletionException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  @Operation(
      summary = "Monitor the given search keys in the key transparency log",
      description = """
          Enforced unauthenticated endpoint. Return proofs proving that the log tree
          has been constructed correctly in later entries for each of the given search keys .
          """
  )
  @ApiResponse(responseCode = "200", description = "All search keys exist in the log", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid request. See response for any available details.")
  @ApiResponse(responseCode = "404", description = "At least one search key lookup did not find the key")
  @ApiResponse(responseCode = "429", description = "Rate-limited")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @POST
  @Path("/monitor")
  @RateLimitedByIp(RateLimiters.For.KEY_TRANSPARENCY_MONITOR_PER_IP)
  @Produces(MediaType.APPLICATION_JSON)
  public KeyTransparencyMonitorResponse monitor(
      @ReadOnly @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final KeyTransparencyMonitorRequest request) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      final List<MonitorKey> monitorKeys = new ArrayList<>(List.of(
          createMonitorKey(getFullSearchKeyByteString(ACI_PREFIX, request.aci().toCompactByteArray()),
              request.aciPositions())
      ));

      request.usernameHash().ifPresent(usernameHash ->
          monitorKeys.add(createMonitorKey(getFullSearchKeyByteString(USERNAME_PREFIX, usernameHash),
              request.usernameHashPositions().get()))
      );

      request.e164().ifPresent(e164 ->
          monitorKeys.add(
              createMonitorKey(getFullSearchKeyByteString(E164_PREFIX, e164.getBytes(StandardCharsets.UTF_8)),
                  request.e164Positions().get()))
      );

      return new KeyTransparencyMonitorResponse(keyTransparencyServiceClient.monitor(
          monitorKeys,
          request.lastNonDistinguishedTreeHeadSize(),
          request.lastDistinguishedTreeHeadSize(),
          KEY_TRANSPARENCY_RPC_TIMEOUT).join());
    } catch (final CancellationException exception) {
      LOGGER.error("Unexpected cancellation from key transparency service", exception);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, exception);
    } catch (final CompletionException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  @Operation(
      summary = "Get the current value of the distinguished key",
      description = """
          Enforced unauthenticated endpoint. The response contains the distinguished tree head to prove consistency
          against for future calls to `/search` and `/distinguished`.
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
  public KeyTransparencyDistinguishedKeyResponse getDistinguishedKey(
      @ReadOnly @Auth final Optional<AuthenticatedDevice> authenticatedAccount,

      @Parameter(description = "The distinguished tree head size returned by a previously verified call")
      @QueryParam("lastTreeHeadSize") @Valid final Optional<@Positive Long> lastTreeHeadSize) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    try {
      return keyTransparencyServiceClient.getDistinguishedKey(lastTreeHeadSize, KEY_TRANSPARENCY_RPC_TIMEOUT)
          .thenApply(KeyTransparencyDistinguishedKeyResponse::new)
          .join();
    } catch (final CancellationException exception) {
      LOGGER.error("Unexpected cancellation from key transparency service", exception);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, exception);
    } catch (final CompletionException exception) {
      handleKeyTransparencyServiceError(exception);
    }
    // This is unreachable
    return null;
  }

  private void handleKeyTransparencyServiceError(final CompletionException exception) {
    final Throwable unwrapped = ExceptionUtils.unwrap(exception);

    if (unwrapped instanceof StatusRuntimeException e) {
      final Status.Code code = e.getStatus().getCode();
      final String description = e.getStatus().getDescription();
      switch (code) {
        case NOT_FOUND -> throw new NotFoundException(description);
        case PERMISSION_DENIED -> throw new ForbiddenException(description);
        case INVALID_ARGUMENT -> throw new WebApplicationException(description, 422);
        default -> throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, unwrapped);
      }
    }
    LOGGER.error("Unexpected key transparency service failure", unwrapped);
    throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, unwrapped);
  }

  private static MonitorKey createMonitorKey(final ByteString fullSearchKey, final List<Long> positions) {
    return MonitorKey.newBuilder()
        .setSearchKey(fullSearchKey)
        .addAllEntries(positions)
        .build();
  }

  private void requireNotAuthenticated(final Optional<AuthenticatedDevice> authenticatedAccount) {
    if (authenticatedAccount.isPresent()) {
      throw new BadRequestException("Endpoint requires unauthenticated access");
    }
  }

  @VisibleForTesting
  static ByteString getFullSearchKeyByteString(final byte prefix, final byte[] searchKeyBytes) {
    final ByteBuffer fullSearchKeyBuffer = ByteBuffer.allocate(searchKeyBytes.length + 1);
    fullSearchKeyBuffer.put(prefix);
    fullSearchKeyBuffer.put(searchKeyBytes);
    fullSearchKeyBuffer.flip();

    return ByteString.copyFrom(fullSearchKeyBuffer.array());
  }
}
