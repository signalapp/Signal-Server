/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getClientPlatform;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.chat.errors.NotFound;
import org.signal.chat.purchase.ConfirmPayPalBoostRequest;
import org.signal.chat.purchase.ConfirmPayPalBoostResponse;
import org.signal.chat.purchase.CreateBoostReceiptCredentialRequest;
import org.signal.chat.purchase.CreateBoostReceiptCredentialResponse;
import org.signal.chat.purchase.CreateStripeBoostRequest;
import org.signal.chat.purchase.CreateStripeBoostResponse;
import org.signal.chat.purchase.CreatePayPalBoostRequest;
import org.signal.chat.purchase.CreatePayPalBoostResponse;
import org.signal.chat.purchase.PaymentRequired;
import org.signal.chat.purchase.SimpleOneTimeDonationsGrpc;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.purchases.BraintreeManager;
import org.whispersystems.textsecuregcm.purchases.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.purchases.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.purchases.PaymentDetails;
import org.whispersystems.textsecuregcm.purchases.PaymentProvider;
import org.whispersystems.textsecuregcm.purchases.PurchaseAmountTooLargeException;
import org.whispersystems.textsecuregcm.purchases.PurchaseNotFoundException;
import org.whispersystems.textsecuregcm.purchases.PurchasePaymentRequiredException;
import org.whispersystems.textsecuregcm.purchases.PurchaseProcessorException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptRequestedForOpenPaymentException;
import org.whispersystems.textsecuregcm.purchases.StripeManager;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;

public class OneTimeDonationsGrpcService extends SimpleOneTimeDonationsGrpc.OneTimeDonationsImplBase {

  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final PayPalDonationsTranslator payPalDonationsTranslator;
  private final OneTimeDonationsManager oneTimeDonationsManager;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final Clock clock;
  private final RateLimiters rateLimiters;
  private final DonationPermitsManager donationPermitsManager;

  public OneTimeDonationsGrpcService(
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final StripeManager stripeManager,
      final BraintreeManager braintreeManager,
      final PayPalDonationsTranslator payPalDonationsTranslator,
      final OneTimeDonationsManager oneTimeDonationsManager,
      final IssuedReceiptsManager issuedReceiptsManager,
      final ServerZkReceiptOperations zkReceiptOperations,
      final Clock clock,
      final RateLimiters rateLimiters,
      final DonationPermitsManager donationPermitsManager) {
    this.oneTimeDonationConfiguration = oneTimeDonationConfiguration;
    this.stripeManager = stripeManager;
    this.braintreeManager = braintreeManager;
    this.payPalDonationsTranslator = payPalDonationsTranslator;
    this.oneTimeDonationsManager = oneTimeDonationsManager;
    this.issuedReceiptsManager = issuedReceiptsManager;
    this.zkReceiptOperations = zkReceiptOperations;
    this.clock = clock;
    this.rateLimiters = rateLimiters;
    this.donationPermitsManager = donationPermitsManager;
  }

  @Override
  public CreateStripeBoostResponse createStripeBoost(final CreateStripeBoostRequest request)
      throws RateLimitExceededException {
    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.forDescriptor(RateLimiters.For.ONE_TIME_DONATION));

    final org.whispersystems.textsecuregcm.purchases.PaymentMethod paymentMethod =
        switch (request.getPaymentMethod()) {
          case PAYMENT_METHOD_CARD -> org.whispersystems.textsecuregcm.purchases.PaymentMethod.CARD;
          case PAYMENT_METHOD_SEPA_DEBIT -> org.whispersystems.textsecuregcm.purchases.PaymentMethod.SEPA_DEBIT;
          case PAYMENT_METHOD_IDEAL -> org.whispersystems.textsecuregcm.purchases.PaymentMethod.IDEAL;
          default -> throw new IllegalStateException("payment_method should already have been validated");
        };

    try {
      OneTimeDonationUtil.validateOneTimeDonationRequest(
          request.getCurrency(), request.getAmount(), request.getLevel(), paymentMethod, oneTimeDonationConfiguration, stripeManager);
      final DonationPermit donationPermit = new DonationPermit(request.getDonationPermit().toByteArray());
      if (!donationPermitsManager.spend(donationPermit)) {
        return CreateStripeBoostResponse.newBuilder()
            .setPermitRejected(FailedZkAuthentication.newBuilder()
                .setDescription("donation permit rejected")
                .build())
            .build();
      }
    } catch (InvalidInputException | VerificationFailedException _) {
      return CreateStripeBoostResponse.newBuilder()
          .setPermitRejected(FailedZkAuthentication.newBuilder()
              .setDescription("donation permit rejected")
              .build())
          .build();
    } catch (OneTimeDonationUtil.ValidationException e) {
      return CreateStripeBoostResponse.newBuilder()
          .setInvalidSelection(FailedPrecondition.newBuilder().setDescription(e.getDescription()).build())
          .build();
    }

    try {
      final com.stripe.model.PaymentIntent paymentIntent = stripeManager.createPaymentIntent(
          request.getCurrency(), request.getAmount(), request.getLevel(),
          getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null)));
      return CreateStripeBoostResponse.newBuilder().setClientSecret(paymentIntent.getClientSecret()).build();
    } catch (final PurchaseAmountTooLargeException e) {
      return CreateStripeBoostResponse.newBuilder()
          .setInvalidSelection(FailedPrecondition.newBuilder()
              .setDescription("Amount exceeds the payment processor's maximum for currency " + request.getCurrency()))
          .build();
    }
  }

  @Override
  public CreatePayPalBoostResponse createPayPalBoost(final CreatePayPalBoostRequest request) throws IOException {

    try {
      OneTimeDonationUtil.validateOneTimeDonationRequest(request.getCurrency(), request.getAmount(), request.getLevel(), org.whispersystems.textsecuregcm.purchases.PaymentMethod.PAYPAL, oneTimeDonationConfiguration, braintreeManager);
    } catch (OneTimeDonationUtil.ValidationException e) {
      return CreatePayPalBoostResponse.newBuilder()
          .setInvalidSelection(FailedPrecondition.newBuilder().setDescription(e.getDescription()).build())
          .build();
    }

    final List<Locale> acceptableLocales = RequestAttributesUtil.getAvailableAcceptedLocales();
    final OneTimeDonationUtil.LocalizedPayPalDonationLineItem localizedLineItem = OneTimeDonationUtil.localizePayPalDonationLineItem(
        payPalDonationsTranslator, acceptableLocales);
    final BraintreeManager.PayPalOneTimePaymentApprovalDetails approvalDetails =
        braintreeManager.createOneTimePayment(
            request.getCurrency().toUpperCase(Locale.ROOT), request.getAmount(),
            localizedLineItem.locale().toLanguageTag(), request.getReturnUrl(), request.getCancelUrl(),
            localizedLineItem.itemName());
    return CreatePayPalBoostResponse.newBuilder()
        .setResult(CreatePayPalBoostResponse.CreatePayPalBoostResult.newBuilder()
            .setApprovalUrl(approvalDetails.approvalUrl())
            .setPaymentId(approvalDetails.paymentId()).build()).build();
  }

  @Override
  public ConfirmPayPalBoostResponse confirmPayPalBoost(final ConfirmPayPalBoostRequest request)
      throws RateLimitExceededException, IOException {
    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.forDescriptor(RateLimiters.For.ONE_TIME_DONATION));

    try {
      OneTimeDonationUtil.validateOneTimeDonationRequest(request.getCurrency(), request.getAmount(), request.getLevel(),
          org.whispersystems.textsecuregcm.purchases.PaymentMethod.PAYPAL, oneTimeDonationConfiguration,
          braintreeManager);
      final BraintreeManager.PayPalChargeSuccessDetails chargeSuccessDetails = braintreeManager.captureOneTimePayment(
          request.getPayerId(), request.getPaymentId(), request.getPaymentToken(),
          request.getCurrency(), request.getAmount(), request.getLevel(),
          SubscriptionsUtil.getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null)));
      oneTimeDonationsManager.putPaidAt(chargeSuccessDetails.paymentId(), clock.instant());
      return ConfirmPayPalBoostResponse.newBuilder()
          .setResult(ConfirmPayPalBoostResponse.ConfirmPayPalBoostResult.newBuilder()
              .setPaymentId(chargeSuccessDetails.paymentId()).build()).build();
    } catch (OneTimeDonationUtil.ValidationException e) {
      return ConfirmPayPalBoostResponse.newBuilder()
          .setInvalidSelection(FailedPrecondition.newBuilder().setDescription(e.getDescription()).build())
          .build();
    } catch (final PurchaseProcessorException e) {
      return ConfirmPayPalBoostResponse.newBuilder()
          .setPaymentRequired(PaymentRequired.newBuilder()
              .setChargeFailure(SubscriptionsUtil.toChargeFailure(e.getProcessor(), e.getChargeFailure())))
          .build();
    }
  }

  @Override
  public CreateBoostReceiptCredentialResponse createBoostReceiptCredential(
      final CreateBoostReceiptCredentialRequest request) throws IOException {

    final PaymentProvider processor = PaymentProvider.fromProto(request.getProcessor())
        .orElseThrow(() -> GrpcExceptions.fieldViolation("processor", "Unsupported payment processor"));

    final PaymentDetails paymentDetails;
    try {
      paymentDetails = switch (processor) {
        case STRIPE -> stripeManager.claimOneTimePurchase(request.getPaymentIntentId());
        case BRAINTREE -> braintreeManager.claimOneTimePurchase(request.getPaymentIntentId());
        default -> throw GrpcExceptions.fieldViolation("processor", "Unsupported payment processor");
      };
    } catch (PurchaseNotFoundException _) {
      return CreateBoostReceiptCredentialResponse.newBuilder()
          .setPaymentNotFound(NotFound.getDefaultInstance()).build();
    } catch (PurchaseReceiptRequestedForOpenPaymentException _) {
      return CreateBoostReceiptCredentialResponse.newBuilder()
          .setPaymentStillProcessing(FailedPrecondition.getDefaultInstance()).build();
    } catch (PurchasePaymentRequiredException e) {
      return CreateBoostReceiptCredentialResponse.newBuilder()
          .setPaymentRequired(SubscriptionsUtil.toPaymentRequired(e)).build();
    }

    final OneTimeDonationUtil.DonationLevelDetails levelDetails;
    try {
      levelDetails = OneTimeDonationUtil.getLevelDetails(paymentDetails, oneTimeDonationConfiguration);
    } catch (final OneTimeDonationUtil.InvalidLevelException e) {
      throw GrpcExceptions.unavailable(e.getMessage());
    }

    final ReceiptCredentialRequest receiptCredentialRequest;
    try {
      receiptCredentialRequest = new ReceiptCredentialRequest(
          request.getReceiptCredentialRequest().toByteArray());
    } catch (final InvalidInputException e) {
      throw GrpcExceptions.fieldViolation("receipt_credential_request", "invalid receipt credential request");
    }

    final Instant paidAt = oneTimeDonationsManager.getPaidAt(processor, paymentDetails.id(), paymentDetails.created());
    final Instant expiration = paidAt
        .plus(levelDetails.levelExpiration())
        .truncatedTo(ChronoUnit.DAYS)
        .plus(1, ChronoUnit.DAYS);

    try {
      issuedReceiptsManager.recordOneTimeIssuance(
          paymentDetails.id(), processor, receiptCredentialRequest, expiration);
    } catch (final WriteConflictException e) {
      return CreateBoostReceiptCredentialResponse.newBuilder()
          .setReceiptAlreadyIssued(FailedPrecondition.getDefaultInstance()).build();
    }

    final ReceiptCredentialResponse receiptCredentialResponse;
    try {
      receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
          receiptCredentialRequest, expiration.getEpochSecond(), levelDetails.level().getValue());
    } catch (final VerificationFailedException e) {
      throw GrpcExceptions.fieldViolation("receipt_credential_request",
          "receipt credential request failed verification");
    }

    Metrics.counter(SubscriptionsGrpcService.RECEIPT_ISSUED_COUNTER_NAME,
            Tags.of(
                Tag.of(SubscriptionsGrpcService.PROCESSOR_TAG_NAME, processor.toString()),
                Tag.of(SubscriptionsGrpcService.TYPE_TAG_NAME, "boost"),
                UserAgentTagUtil.getPlatformTag(RequestAttributesUtil.getUserAgent().orElse(null))))
        .increment();

    return CreateBoostReceiptCredentialResponse.newBuilder()
        .setResult(CreateBoostReceiptCredentialResponse.CreateBoostReceiptCredentialResult.newBuilder()
            .setReceiptCredentialResponse(ByteString.copyFrom(receiptCredentialResponse.serialize()))
            .build())
        .build();
  }

}
