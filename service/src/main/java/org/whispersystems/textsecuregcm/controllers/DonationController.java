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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;

@Path("/v1/donation")
@Tag(name = "Donations")
public class DonationController {

  public interface ReceiptCredentialPresentationFactory {
    ReceiptCredentialPresentation build(byte[] bytes) throws InvalidInputException;
  }

  private final Clock clock;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final RedeemedReceiptsManager redeemedReceiptsManager;
  private final AccountsManager accountsManager;
  private final BadgesConfiguration badgesConfiguration;
  private final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;

  public DonationController(
      @Nonnull final Clock clock,
      @Nonnull final ServerZkReceiptOperations serverZkReceiptOperations,
      @Nonnull final RedeemedReceiptsManager redeemedReceiptsManager,
      @Nonnull final AccountsManager accountsManager,
      @Nonnull final BadgesConfiguration badgesConfiguration,
      @Nonnull final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory) {
    this.clock = Objects.requireNonNull(clock);
    this.serverZkReceiptOperations = Objects.requireNonNull(serverZkReceiptOperations);
    this.redeemedReceiptsManager = Objects.requireNonNull(redeemedReceiptsManager);
    this.accountsManager = Objects.requireNonNull(accountsManager);
    this.badgesConfiguration = Objects.requireNonNull(badgesConfiguration);
    this.receiptCredentialPresentationFactory = Objects.requireNonNull(receiptCredentialPresentationFactory);
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
  public CompletionStage<Response> redeemReceipt(
      @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final RedeemReceiptRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      ReceiptCredentialPresentation receiptCredentialPresentation;
      try {
        receiptCredentialPresentation = receiptCredentialPresentationFactory
            .build(request.getReceiptCredentialPresentation());
      } catch (InvalidInputException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST)
            .entity("invalid receipt credential presentation")
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build());
      }
      try {
        serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
      } catch (VerificationFailedException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST)
            .entity("receipt credential presentation verification failed")
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build());
      }

      final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
      final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
      final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
      final String badgeId = badgesConfiguration.getReceiptLevels().get(receiptLevel);
      if (badgeId == null) {
        return CompletableFuture.completedFuture(Response.serverError()
            .entity("server does not recognize the requested receipt level")
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build());
      }

      return accountsManager.getByAccountIdentifierAsync(auth.accountIdentifier())
          .thenCompose(maybeAccount -> {
            final Account account = maybeAccount.orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

            return redeemedReceiptsManager.put(
                    receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, auth.accountIdentifier())
                .thenCompose(receiptMatched -> {
                  if (!receiptMatched) {
                    return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST)
                        .entity("receipt serial is already redeemed")
                        .type(MediaType.TEXT_PLAIN_TYPE)
                        .build());
                  }

                  return accountsManager.updateAsync(account, a -> {
                        a.addBadge(clock, new AccountBadge(badgeId, receiptExpiration, request.isVisible()));
                        if (request.isPrimary()) {
                          a.makeBadgePrimaryIfExists(clock, badgeId);
                        }
                      })
                      .thenApply(ignored -> Response.ok().build());
                });
          });
    }).thenCompose(Function.identity());
  }

}
