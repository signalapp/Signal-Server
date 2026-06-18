/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermitDerivedKeyPair;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.CreateDonationPermitResponse;
import org.whispersystems.textsecuregcm.entities.CreateDonationPermitsRequest;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptCredentialPresentationFactory;

@Path("/v1/donation")
@Tag(name = "Donations")
public class DonationController {

  private final Clock clock;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final RedeemedReceiptsManager redeemedReceiptsManager;
  private final AccountsManager accountsManager;
  private final BadgesConfiguration badgesConfiguration;
  private final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  private final ServerSecretParams serverSecretParams;
  private final RateLimiters rateLimiters;

  public DonationController(
      final Clock clock,
      final ServerZkReceiptOperations serverZkReceiptOperations,
      final RedeemedReceiptsManager redeemedReceiptsManager,
      final AccountsManager accountsManager,
      final BadgesConfiguration badgesConfiguration,
      final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory,
      final ServerSecretParams serverSecretParams,
      final RateLimiters rateLimiters) {
    this.clock = Objects.requireNonNull(clock);
    this.serverZkReceiptOperations = Objects.requireNonNull(serverZkReceiptOperations);
    this.redeemedReceiptsManager = Objects.requireNonNull(redeemedReceiptsManager);
    this.accountsManager = Objects.requireNonNull(accountsManager);
    this.badgesConfiguration = Objects.requireNonNull(badgesConfiguration);
    this.receiptCredentialPresentationFactory = Objects.requireNonNull(receiptCredentialPresentationFactory);
    this.serverSecretParams = Objects.requireNonNull(serverSecretParams);
    this.rateLimiters = Objects.requireNonNull(rateLimiters);
  }

  @POST
  @Path("/redeem-receipt")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @Operation(
      summary = "Redeem receipt",
      description = """
          Redeem a receipt acquired from /v1/subscription/{subscriberId}/receipt_credentials to add a badge to the
          account. After successful redemption, profile responses will include the corresponding badge (if configured as
          visible) until the expiration time on the receipt.
          """)
  @ApiResponse(responseCode = "200", description = "The receipt was redeemed")
  @ApiResponse(responseCode = "400", description = """
      The provided presentation or receipt was invalid, or the receipt was already redeemed for a different account. A
      specific error message suitable for logging will be included as text/plain body
      """)
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ManagedAsync
  public Response redeemReceipt(
      @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final RedeemReceiptRequest request) {
    ReceiptCredentialPresentation receiptCredentialPresentation;
    try {
      receiptCredentialPresentation = receiptCredentialPresentationFactory
          .build(request.getReceiptCredentialPresentation());
    } catch (InvalidInputException e) {
      return Response.status(Status.BAD_REQUEST)
          .entity("invalid receipt credential presentation")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }
    try {
      serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
    } catch (VerificationFailedException e) {
      return Response.status(Status.BAD_REQUEST)
          .entity("receipt credential presentation verification failed")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
    final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
    final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
    final String badgeId = badgesConfiguration.getReceiptLevels().get(receiptLevel);
    if (badgeId == null) {
      return Response.serverError()
          .entity("server does not recognize the requested receipt level")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    final boolean receiptMatched = redeemedReceiptsManager.put(
        receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, auth.accountIdentifier());
    if (!receiptMatched) {
      return Response.status(Status.BAD_REQUEST)
          .entity("receipt serial is already redeemed")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    accountsManager.update(auth.accountIdentifier(), a -> {
      a.addBadge(clock, new AccountBadge(badgeId, receiptExpiration, request.isVisible()));
      if (request.isPrimary()) {
        a.makeBadgePrimaryIfExists(clock, badgeId);
      }
    });
    return Response.ok().build();
  }

  @POST
  @Path("/permit")
  @Produces({MediaType.APPLICATION_JSON})
  @Operation(
      summary = "Generate permits for anonymous donation endpoints",
      description = """
          Generate a set of anonymous, single-use, permits for use with /v1/subscription endpoints.
          """)
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid credential request")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed")
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  @ApiResponse(responseCode = "429", description = "Rate-limited; reduce requested permit count and/or try again after the prescribed delay")
  public CreateDonationPermitResponse createPermits(@Auth final AuthenticatedDevice auth,
      @NotNull @Valid final CreateDonationPermitsRequest request) throws RateLimitExceededException {

    final DonationPermitRequest permitRequest;
    try {
      permitRequest = new DonationPermitRequest(request.permitRequest());
    } catch (InvalidInputException e) {
      throw new BadRequestException();
    }

    rateLimiters.getCreateDonationPermitLimiter().validate(auth.accountIdentifier(), permitRequest.getPermitCount());

    final DonationPermitDerivedKeyPair derivedKeyPair = DonationPermitDerivedKeyPair.forExpiration(
        DonationPermitResponse.defaultExpiration(clock.instant()), serverSecretParams);

    final DonationPermitResponse permitResponse = permitRequest.issue(derivedKeyPair);

    return new CreateDonationPermitResponse(permitResponse.serialize());
  }

}
