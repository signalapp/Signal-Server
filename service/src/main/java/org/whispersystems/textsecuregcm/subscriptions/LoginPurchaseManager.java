/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import com.google.common.annotations.VisibleForTesting;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;

public class LoginPurchaseManager {

  private final Map<PaymentProvider, OneTimePaymentProcessor> oneTimePaymentProcessors;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final long loginLevel;
  private final Clock clock;

  // Signal Login receipt credentials expire after 5 years
  @VisibleForTesting
  final static Duration LOGIN_EXPIRATION = Duration.ofDays(366 * 5);

  /// Construct a LoginPurchaseManager
  ///
  /// @param oneTimePaymentProcessors The processor to use for each supported [PaymentProvider]
  /// @param issuedReceiptsManager    Tracks which purchases have already had receipt credentials issued for them
  /// @param zkReceiptOperations      Used to issue receipt credentials
  /// @param loginLevel               The receipt level that identifies a Signal Login purchase. Purchases for any other
  ///                                 level are rejected.
  /// @param clock                    A clock
  public LoginPurchaseManager(
      final Map<PaymentProvider, OneTimePaymentProcessor> oneTimePaymentProcessors,
      final IssuedReceiptsManager issuedReceiptsManager,
      final ServerZkReceiptOperations zkReceiptOperations,
      final long loginLevel,
      final Clock clock) {
    this.oneTimePaymentProcessors = oneTimePaymentProcessors;
    this.issuedReceiptsManager = issuedReceiptsManager;
    this.zkReceiptOperations = zkReceiptOperations;
    this.loginLevel = loginLevel;
    this.clock = clock;
  }

  /// Verify a completed one-time purchase and issue a receipt credential for it.
  ///
  /// Repeated calls must use the same `receiptCredentialRequest`; a second request for a purchase that was already
  /// redeemed fails with [SubscriptionReceiptAlreadyRedeemedException].
  ///
  /// @param paymentProvider          The provider that processed the purchase
  /// @param purchaseId               The identifier for the purchase in the `paymentProvider`
  /// @param receiptCredentialRequest The request for the receipt to generate. All retries for the same purchaseId must
  /// use the same request
  /// @return The receipt credential
  ///
  public ReceiptCredentialResponse generateReceipt(
      final PaymentProvider paymentProvider,
      final String purchaseId,
      final ReceiptCredentialRequest receiptCredentialRequest)
      throws RateLimitExceededException, SubscriptionInvalidArgumentsException, IOException, SubscriptionNotFoundException, SubscriptionReceiptRequestedForOpenPaymentException, SubscriptionPaymentRequiredException, SubscriptionReceiptAlreadyRedeemedException, VerificationFailedException {

    final OneTimePaymentProcessor oneTimePaymentProcessor = oneTimePaymentProcessors.get(paymentProvider);
    if (oneTimePaymentProcessor == null) {
      throw new SubscriptionInvalidArgumentsException("unknown payment provider: " + paymentProvider);
    }

    final PaymentDetails paymentDetails = oneTimePaymentProcessor
        .claimOneTimePurchase(purchaseId)
        .orElseThrow(SubscriptionNotFoundException::new);
    if (paymentDetails.status() == PaymentStatus.PROCESSING) {
      throw new SubscriptionReceiptRequestedForOpenPaymentException();
    } else if (paymentDetails.status() != PaymentStatus.SUCCEEDED) {
      throw Optional.ofNullable(paymentDetails.chargeFailure())
          .<SubscriptionPaymentRequiredException>map(
              cf -> new SubscriptionChargeFailurePaymentRequiredException(paymentProvider, cf))
          .orElseGet(SubscriptionPaymentRequiredException::new);
    } else if (paymentDetails.level() == null || paymentDetails.level() != loginLevel) {
      throw new SubscriptionInvalidArgumentsException("purchase was for an unexpected product");
    }

    try {
      issuedReceiptsManager.recordOneTimeIssuance(paymentDetails.id(), paymentProvider, receiptCredentialRequest,
          clock.instant());
    } catch (WriteConflictException _) {
      throw new SubscriptionReceiptAlreadyRedeemedException();
    }

    // Calculating the expiration from the creation date works for IAP purchases. However, for other processors, the
    // creation date of the payment intent might be days before the payment actually completed. If we support non-IAP
    // processors we should attempt to get the latest date. see OneTimeDonationController/OneTimeDonationManager
    final Instant expiration = paymentDetails.created().plus(LOGIN_EXPIRATION).truncatedTo(ChronoUnit.DAYS);

    return zkReceiptOperations.issueReceiptCredential(
        receiptCredentialRequest, expiration.getEpochSecond(), loginLevel);
  }
}
