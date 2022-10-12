/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;

@Path("/v1/donation")
public class DonationController {

  public interface ReceiptCredentialPresentationFactory {
    ReceiptCredentialPresentation build(byte[] bytes) throws InvalidInputException;
  }

  private static final Logger logger = LoggerFactory.getLogger(DonationController.class);

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

  @Timed
  @POST
  @Path("/redeem-receipt")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public CompletionStage<Response> redeemReceipt(
      @Auth final AuthenticatedAccount auth,
      @NotNull @Valid final RedeemReceiptRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      ReceiptCredentialPresentation receiptCredentialPresentation;
      try {
        receiptCredentialPresentation = receiptCredentialPresentationFactory.build(
            request.getReceiptCredentialPresentation());
      } catch (InvalidInputException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).entity("invalid receipt credential presentation").type(MediaType.TEXT_PLAIN_TYPE).build());
      }
      try {
        serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
      } catch (VerificationFailedException e) {
        return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).entity("receipt credential presentation verification failed").type(MediaType.TEXT_PLAIN_TYPE).build());
      }

      final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
      final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
      final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
      final String badgeId = badgesConfiguration.getReceiptLevels().get(receiptLevel);
      if (badgeId == null) {
        return CompletableFuture.completedFuture(Response.serverError().entity("server does not recognize the requested receipt level").type(MediaType.TEXT_PLAIN_TYPE).build());
      }
      final CompletionStage<Boolean> putStage = redeemedReceiptsManager.put(
          receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, auth.getAccount().getUuid());
      return putStage.thenApplyAsync(receiptMatched -> {
        if (!receiptMatched) {
          return Response.status(Status.BAD_REQUEST).entity("receipt serial is already redeemed").type(MediaType.TEXT_PLAIN_TYPE).build();
        }

        try {
          ForkJoinPool.managedBlock(new ManagedBlocker() {
            boolean done = false;

            @Override
            public boolean block() {
              final Optional<Account> optionalAccount = accountsManager.getByAccountIdentifier(auth.getAccount().getUuid());
              optionalAccount.ifPresent(account -> {
                accountsManager.update(account, a -> {
                  a.addBadge(clock, new AccountBadge(badgeId, receiptExpiration, request.isVisible()));
                  if (request.isPrimary()) {
                    a.makeBadgePrimaryIfExists(clock, badgeId);
                  }
                });
              });
              done = true;
              return true;
            }

            @Override
            public boolean isReleasable() {
              return done;
            }
          });
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return Response.serverError().build();
        }

        return Response.ok().build();
      });
    }).thenCompose(Function.identity());
  }

}
