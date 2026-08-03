/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Optional;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.mappers.SubscriptionExceptionMapper;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.subscriptions.LoginPurchaseManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionInvalidArgumentsException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionNotFoundException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentRequiredException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionReceiptRequestedForOpenPaymentException;

@Path("/v1/login-purchase")
@io.swagger.v3.oas.annotations.tags.Tag(name = "LoginPurchase")
public class LoginPurchaseController {

  private final LoginPurchaseManager loginPurchaseManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public LoginPurchaseController(
      final LoginPurchaseManager loginPurchaseManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.loginPurchaseManager = loginPurchaseManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public record CreateLoginReceiptCredentialRequest(
      @NotNull String purchaseIdentifier,
      @NotNull byte[] receiptCredentialRequest,
      @NotNull PaymentProvider paymentProvider) {
  }

  public record CreateLoginReceiptCredentialResponse(byte[] receiptCredentialResponse) {
  }

  @POST
  @Path("/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a receipt credential for a completed one-time login purchase",
      description = """
          Verify a completed one-time purchase with the payment provider and issue a receipt credential that can be
          redeemed for a login.

          This endpoint must be called on an unauthenticated connection. Retries for the same purchaseIdentifier MUST
          use the same receiptCredentialRequest.
          """)
  @ApiResponse(responseCode = "200", description = "Successfully created receipt",
      content = @Content(schema = @Schema(implementation = CreateLoginReceiptCredentialResponse.class)))
  @ApiResponse(responseCode = "204", description = "The purchase is still pending with the payment provider. The client may retry later.")
  @ApiResponse(responseCode = "400", description = """
      Invalid request: malformed request, failed zkgroup verification, unsupported paymentProvider, the provided
      purchase identifier not for a Signal Login, or login purchases are not currently enabled.
      """)
  @ApiResponse(responseCode = "402", description = "The purchase did not complete successfully. The body may include ChargeFailure details.",
      content = @Content(schema = @Schema(
          nullable = true,
          implementation = SubscriptionExceptionMapper.ChargeFailureResponse.class)))
  @ApiResponse(responseCode = "403", description = "The request was made on an authenticated channel")
  @ApiResponse(responseCode = "404", description = "The payment provider has no purchase with the provided purchaseIdentifier")
  @ApiResponse(responseCode = "409", description = "The purchase was already redeemed for a receipt credential, but with a different receipt credential request")
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, a positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  @ManagedAsync
  public Response createLoginReceiptCredential(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,
      @NotNull @Valid final CreateLoginReceiptCredentialRequest request)
      throws IOException, SubscriptionPaymentRequiredException, SubscriptionInvalidArgumentsException, SubscriptionNotFoundException, RateLimitExceededException, SubscriptionReceiptAlreadyRedeemedException {

    if (!dynamicConfigurationManager.getConfiguration().getLoginPurchaseConfiguration().enabled()) {
      throw new BadRequestException("login purchases are not enabled");
    }

    if (authenticatedAccount.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for login purchase operations");
    }

    final ReceiptCredentialRequest receiptCredentialRequest;
    try {
      receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest);
    } catch (final InvalidInputException e) {
      throw new BadRequestException("invalid receipt credential request", e);
    }

    try {
      final ReceiptCredentialResponse receiptCredentialResponse = loginPurchaseManager.generateReceipt(
          request.paymentProvider, request.purchaseIdentifier, receiptCredentialRequest);
      return Response.ok(
              new CreateLoginReceiptCredentialResponse(receiptCredentialResponse.serialize()))
          .build();
    } catch (SubscriptionReceiptRequestedForOpenPaymentException e) {
      return Response.noContent().build();
    } catch (VerificationFailedException e) {
      throw new BadRequestException("receipt credential request failed verification", e);
    }
  }
}
