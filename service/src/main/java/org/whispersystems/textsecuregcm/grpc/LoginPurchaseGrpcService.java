/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.NotFound;
import org.signal.chat.purchase.CreateLoginReceiptCredentialRequest;
import org.signal.chat.purchase.CreateLoginReceiptCredentialResponse;
import org.signal.chat.purchase.SimpleLoginPurchaseGrpc;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.purchases.LoginPurchaseManager;
import org.whispersystems.textsecuregcm.purchases.PaymentProvider;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidArgumentsException;
import org.whispersystems.textsecuregcm.purchases.PurchaseNotFoundException;
import org.whispersystems.textsecuregcm.purchases.PurchasePaymentRequiredException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptRequestedForOpenPaymentException;

public class LoginPurchaseGrpcService extends SimpleLoginPurchaseGrpc.LoginPurchaseImplBase {

  private final LoginPurchaseManager loginPurchaseManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public LoginPurchaseGrpcService(
      final LoginPurchaseManager loginPurchaseManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.loginPurchaseManager = loginPurchaseManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public CreateLoginReceiptCredentialResponse createLoginReceiptCredential(
      final CreateLoginReceiptCredentialRequest request) throws IOException, RateLimitExceededException {

    if (!dynamicConfigurationManager.getConfiguration().getLoginPurchaseConfiguration().enabled()) {
      throw GrpcExceptions.invalidArguments("method not allowed");
    }

    final PaymentProvider paymentProvider = PaymentProvider.fromProto(request.getProcessor())
        .orElseThrow(() -> GrpcExceptions.fieldViolation("payment_provider", "Unrecognized payment provider"));

    final ReceiptCredentialRequest receiptCredentialRequest;
    try {
      receiptCredentialRequest = new ReceiptCredentialRequest(request.getReceiptCredentialRequest().toByteArray());
    } catch (final InvalidInputException e) {
      throw GrpcExceptions.fieldViolation("receipt_credential_request", "invalid receipt credential request");
    }

    try {
      final ReceiptCredentialResponse receiptCredentialResponse = loginPurchaseManager.generateReceipt(
          paymentProvider, request.getPurchaseIdentifier(), receiptCredentialRequest);

      return CreateLoginReceiptCredentialResponse.newBuilder()
          .setResult(CreateLoginReceiptCredentialResponse.CreateLoginReceiptCredentialResult.newBuilder()
              .setReceiptCredentialResponse(ByteString.copyFrom(receiptCredentialResponse.serialize())))
          .build();
    } catch (final PurchaseReceiptRequestedForOpenPaymentException e) {
      return CreateLoginReceiptCredentialResponse.newBuilder()
          .setPaymentStillProcessing(FailedPrecondition.getDefaultInstance())
          .build();
    } catch (final PurchasePaymentRequiredException e) {
      return CreateLoginReceiptCredentialResponse.newBuilder()
          .setPaymentRequired(SubscriptionsUtil.toPaymentRequired(e))
          .build();
    } catch (final PurchaseNotFoundException e) {
      return CreateLoginReceiptCredentialResponse.newBuilder()
          .setPaymentNotFound(NotFound.getDefaultInstance())
          .build();
    } catch (final PurchaseReceiptAlreadyRedeemedException e) {
      return CreateLoginReceiptCredentialResponse.newBuilder()
          .setReceiptAlreadyIssued(FailedPrecondition.getDefaultInstance())
          .build();
    } catch (final PurchaseInvalidArgumentsException e) {
      throw GrpcExceptions.invalidArguments(e.errorDetail().orElse(""));
    } catch (final VerificationFailedException e) {
      throw GrpcExceptions.fieldViolation("receipt_credential_request",
          "receipt credential request failed verification");
    }
  }
}
