/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;
import org.glassfish.jersey.server.ManagedAsync;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.devicecheck.AppleDeviceCheckManager;
import org.whispersystems.textsecuregcm.storage.devicecheck.ChallengeNotFoundException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DeviceCheckKeyIdNotFoundException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DeviceCheckVerificationFailedException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DuplicatePublicKeyException;
import org.whispersystems.textsecuregcm.storage.devicecheck.RequestReuseException;
import org.whispersystems.textsecuregcm.storage.devicecheck.TooManyKeysException;
import org.whispersystems.textsecuregcm.util.SystemMapper;

/**
 * Process platform device attestations.
 * <p>
 * Device attestations allow clients that can prove that they are running a signed signal build on valid Apple hardware.
 * Currently, this is only used to allow beta builds to access backup functionality, since in-app purchases are not
 * available iOS TestFlight builds.
 */
@Path("/v1/devicecheck")
@io.swagger.v3.oas.annotations.tags.Tag(name = "DeviceCheck")
public class DeviceCheckController {

  private final Clock clock;
  private final AccountsManager accountsManager;
  private final BackupAuthManager backupAuthManager;
  private final AppleDeviceCheckManager deviceCheckManager;
  private final RateLimiters rateLimiters;
  private final long backupRedemptionLevel;
  private final Duration backupRedemptionDuration;

  public DeviceCheckController(
      final Clock clock,
      final AccountsManager accountsManager,
      final BackupAuthManager backupAuthManager,
      final AppleDeviceCheckManager deviceCheckManager,
      final RateLimiters rateLimiters,
      final long backupRedemptionLevel,
      final Duration backupRedemptionDuration) {
    this.clock = clock;
    this.accountsManager = accountsManager;
    this.backupAuthManager = backupAuthManager;
    this.deviceCheckManager = deviceCheckManager;
    this.backupRedemptionLevel = backupRedemptionLevel;
    this.backupRedemptionDuration = backupRedemptionDuration;
    this.rateLimiters = rateLimiters;
  }

  public record ChallengeResponse(
      @Schema(description = "A challenge to use when generating attestations or assertions")
      String challenge) {}

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/attest")
  @Operation(summary = "Fetch an attest challenge", description = """
      Retrieve a challenge to use in an attestation, which should be provided at `PUT /v1/devicecheck/attest`. To
      produce the clientDataHash for [attestKey](https://developer.apple.com/documentation/devicecheck/dcappattestservice/attestkey(_:clientdatahash:completionhandler:))
      take the SHA256 of the UTF-8 bytes of the returned challenge.
      
      Repeat calls to retrieve a challenge may return the same challenge until it is used in a `PUT`. Callers should
      have a single outstanding challenge at any given time.
      """)
  @ApiResponse(responseCode = "200", description = "The response body includes a challenge")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  @ManagedAsync
  public ChallengeResponse attestChallenge(@Auth AuthenticatedDevice authenticatedDevice)
      throws RateLimitExceededException {
    rateLimiters.forDescriptor(RateLimiters.For.DEVICE_CHECK_CHALLENGE)
        .validate(authenticatedDevice.accountIdentifier());

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    return new ChallengeResponse(deviceCheckManager.createChallenge(
        AppleDeviceCheckManager.ChallengeType.ATTEST,
        account));
  }

  @PUT
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/attest")
  @Operation(summary = "Register a keyId", description = """
      Register a keyId with an attestation, which can be used to generate assertions from this account.
      
      The attestation should use the SHA-256 of a challenge retrieved at `GET /v1/devicecheck/attest` as the
      `clientDataHash`
      
      Registration is idempotent, and you should retry network errors with the same challenge as suggested by [device
      check](https://developer.apple.com/documentation/devicecheck/dcappattestservice/attestkey(_:clientdatahash:completionhandler:)#discussion),
      as long as your challenge has not expired (410). Even if your challenge is expired, you may continue to retry with
      your original keyId (and a fresh challenge).
      """)
  @ApiResponse(responseCode = "204", description = "The keyId was successfully added to the account")
  @ApiResponse(responseCode = "410", description = "There was no challenge associated with the account. It may have expired.")
  @ApiResponse(responseCode = "401", description = "The attestation could not be verified")
  @ApiResponse(responseCode = "413", description = "There are too many unique keyIds associated with this account. This is an unrecoverable error.")
  @ApiResponse(responseCode = "409", description = "The provided keyId has already been registered to a different account")
  @ManagedAsync
  public void attest(
      @Auth final AuthenticatedDevice authenticatedDevice,

      @Valid
      @NotNull
      @Parameter(description = "The keyId, encoded with padded url-safe base64")
      @QueryParam("keyId") final String keyId,

      @RequestBody(description = "The attestation data, created by [attestKey](https://developer.apple.com/documentation/devicecheck/dcappattestservice/attestkey(_:clientdatahash:completionhandler:))")
      @NotNull final byte[] attestation) {

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    try {
      deviceCheckManager.registerAttestation(account, parseKeyId(keyId), attestation);
    } catch (TooManyKeysException e) {
      throw new WebApplicationException(Response.status(413).build());
    } catch (ChallengeNotFoundException e) {
      throw new WebApplicationException(Response.status(410).build());
    } catch (DeviceCheckVerificationFailedException e) {
      throw new WebApplicationException(e.getMessage(), Response.status(401).build());
    } catch (DuplicatePublicKeyException e) {
      throw new WebApplicationException(Response.status(409).build());
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/assert")
  @Operation(summary = "Fetch an assert challenge", description = """
      Retrieve a challenge to use in an attestation, which must be provided at `POST /v1/devicecheck/assert`. To produce
      the `clientDataHash` for [generateAssertion](https://developer.apple.com/documentation/devicecheck/dcappattestservice/generateassertion(_:clientdatahash:completionhandler:)),
      construct the request you intend to `POST` and include the returned challenge as the "challenge"
      field. Serialize the request as JSON and take the SHA256 of the request, as described [here](https://developer.apple.com/documentation/devicecheck/establishing-your-app-s-integrity#Assert-your-apps-validity-as-necessary).
      Note that the JSON body provided to the PUT must exactly match the input to the `clientDataHash` (field order,
      whitespace, etc matters)
      
      Repeat calls to retrieve a challenge may return the same challenge until it is used in a `POST`. Callers should
      attempt to only have a single outstanding challenge at any given time.
      """)
  @ApiResponse(responseCode = "200", description = "The response body includes a challenge")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  @ManagedAsync
  public ChallengeResponse assertChallenge(
      @Auth AuthenticatedDevice authenticatedDevice,

      @Parameter(schema = @Schema(description = "The type of action you will make an assertion for",
          allowableValues = {"backup"},
          implementation = String.class))
      @QueryParam("action") Action action) throws RateLimitExceededException {
    rateLimiters.forDescriptor(RateLimiters.For.DEVICE_CHECK_CHALLENGE)
        .validate(authenticatedDevice.accountIdentifier());

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    return new ChallengeResponse(deviceCheckManager.createChallenge(toChallengeType(action), account));
  }

  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/assert")
  @Operation(summary = "Perform an attested action", description = """
      Specify some action to take on the account via the request field. The request must exactly match the request you
      provide when [generating the assertion](https://developer.apple.com/documentation/devicecheck/dcappattestservice/generateassertion(_:clientdatahash:completionhandler:)).
      The request must include a challenge previously retrieved from `GET /v1/devicecheck/assert`.
      
      Each assertion increments the counter associated with the client's device key. This method enforces that no
      assertion with a counter lower than a counter we've already seen is allowed to execute. If a client issues
      multiple requests concurrently, or if they retry a request that had an indeterminate outcome, it's possible that
      the request will not be accepted because the server has already stored the updated counter. In this case the
      request may return 401, and the client should generate a fresh assert for the request.
      """)
  @ApiResponse(responseCode = "204", description = "The assertion was valid and the corresponding action was executed")
  @ApiResponse(responseCode = "404", description = "The provided keyId was not found")
  @ApiResponse(responseCode = "410", description = "There was no challenge associated with the account. It may have expired.")
  @ApiResponse(responseCode = "401", description = "The assertion could not be verified")
  @ManagedAsync
  public void assertion(
      @Auth final AuthenticatedDevice authenticatedDevice,

      @Valid
      @NotNull
      @Parameter(description = "The keyId, encoded with padded url-safe base64")
      @QueryParam("keyId") final String keyId,

      @Valid
      @NotNull
      @Parameter(description = """
          The asserted JSON request data, encoded as a string in padded url-safe base64. This must exactly match the
          request you use when generating the assertion (including field ordering, whitespace, etc).
          """,
          schema = @Schema(implementation = AssertionRequest.class))
      @QueryParam("request") final DeviceCheckController.AssertionRequestWrapper request,

      @RequestBody(description = "The assertion created by [generateAssertion](https://developer.apple.com/documentation/devicecheck/dcappattestservice/generateassertion(_:clientdatahash:completionhandler:))")
      @NotNull final byte[] assertion) {

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    try {
      deviceCheckManager.validateAssert(
          account,
          parseKeyId(keyId),
          toChallengeType(request.assertionRequest().action()),
          request.assertionRequest().challenge(),
          request.rawJson(),
          assertion);
    } catch (ChallengeNotFoundException e) {
      throw new WebApplicationException(Response.status(410).build());
    } catch (DeviceCheckVerificationFailedException e) {
      throw new WebApplicationException(e.getMessage(), Response.status(401).build());
    } catch (DeviceCheckKeyIdNotFoundException | RequestReuseException e) {
      throw new WebApplicationException(Response.status(404).build());
    }

    // The request assertion was validated, execute it
    switch (request.assertionRequest().action()) {
      case BACKUP -> backupAuthManager.extendBackupVoucher(
              account,
              new Account.BackupVoucher(backupRedemptionLevel, clock.instant().plus(backupRedemptionDuration)))
          .join();
    }
  }

  public enum Action {
    BACKUP;

    @JsonCreator
    public static Action fromString(final String action) {
      for (final Action a : Action.values()) {
        if (a.name().toLowerCase(Locale.ROOT).equals(action)) {
          return a;
        }
      }
      throw new IllegalArgumentException("Invalid action: " + action);
    }
  }

  public record AssertionRequest(
      @Schema(description = "The challenge retrieved at `GET /v1/devicecheck/assert`")
      String challenge,
      @Schema(description = "The type of action you'd like to perform with this assert",
          allowableValues = {"backup"}, implementation = String.class)
      Action action) {}

  /*
   * Parses the base64 encoded AssertionRequest, but preserves the rawJson as well
   */
  public record AssertionRequestWrapper(AssertionRequest assertionRequest, byte[] rawJson) {

    public static AssertionRequestWrapper fromString(String requestBase64) throws IOException {
      final byte[] requestJson = Base64.getUrlDecoder().decode(requestBase64);
      final AssertionRequest requestData = SystemMapper.jsonMapper().readValue(requestJson, AssertionRequest.class);
      return new AssertionRequestWrapper(requestData, requestJson);
    }
  }


  private static AppleDeviceCheckManager.ChallengeType toChallengeType(final Action action) {
    return switch (action) {
      case BACKUP -> AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION;
    };
  }

  private static byte[] parseKeyId(final String base64KeyId) {
    try {
      return Base64.getUrlDecoder().decode(base64KeyId);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(Response.status(422).entity(e.getMessage()).build());
    }
  }
}
