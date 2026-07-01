/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.getClientPlatform;
import static org.whispersystems.textsecuregcm.grpc.SubscriptionsUtil.toChargeFailure;

import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.chat.errors.NotFound;
import org.signal.chat.purchase.AmountAboveSepaLimitError;
import org.signal.chat.purchase.AmountBelowMinimumError;
import org.signal.chat.purchase.ConfirmPayPalBoostRequest;
import org.signal.chat.purchase.ConfirmPayPalBoostResponse;
import org.signal.chat.purchase.CreateBoostReceiptCredentialsRequest;
import org.signal.chat.purchase.CreateBoostReceiptCredentialsResponse;
import org.signal.chat.purchase.CreateBoostRequest;
import org.signal.chat.purchase.CreateBoostResponse;
import org.signal.chat.purchase.CreatePayPalBoostRequest;
import org.signal.chat.purchase.CreatePayPalBoostResponse;
import org.signal.chat.purchase.SimpleOneTimeDonationsGrpc;
import org.signal.chat.purchase.PaymentRequired;
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
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;

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
  public CreateBoostResponse createBoost(final CreateBoostRequest request) throws RateLimitExceededException {
    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.forDescriptor(RateLimiters.For.ONE_TIME_DONATION));

    try {
      final DonationPermit donationPermit = new DonationPermit(request.getDonationPermit().toByteArray());
      if (!donationPermitsManager.spend(donationPermit)) {
        return CreateBoostResponse.newBuilder()
            .setPermitRejected(FailedZkAuthentication.newBuilder()
                .setDescription("donation permit rejected")
                .build())
            .build();
      }
    } catch (InvalidInputException | VerificationFailedException _) {
      return CreateBoostResponse.newBuilder()
          .setPermitRejected(FailedZkAuthentication.newBuilder()
              .setDescription("donation permit rejected")
              .build())
          .build();
    }

    final org.whispersystems.textsecuregcm.subscriptions.PaymentMethod paymentMethod =
        switch (request.getPaymentMethod()) {
          case PAYMENT_METHOD_CARD -> org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.CARD;
          case PAYMENT_METHOD_SEPA_DEBIT -> org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.SEPA_DEBIT;
          case PAYMENT_METHOD_IDEAL -> org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.IDEAL;
          default -> throw GrpcExceptions.fieldViolation("payment_method", "Unsupported payment method");
        };

    final OneTimeDonationUtil.OneTimeDonationRequestValidationResult validationResult =
        OneTimeDonationUtil.validateOneTimeDonationRequest(
            request.getCurrency(),
            BigDecimal.valueOf(request.getAmount()),
            request.getLevel(),
            paymentMethod,
            oneTimeDonationConfiguration,
            stripeManager);

    return switch (validationResult) {
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedLevel _ ->
          CreateBoostResponse.newBuilder()
              .setUnsupportedLevel(FailedPrecondition.getDefaultInstance()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedCurrency _ ->
          CreateBoostResponse.newBuilder()
              .setUnsupportedCurrency(FailedPrecondition.getDefaultInstance()).build();
      case final OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountBelowMinimum r ->
          CreateBoostResponse.newBuilder()
              .setAmountBelowMinimum(AmountBelowMinimumError.newBuilder()
                  .setMinimum(r.minimum().toString()).build()).build();
      case final OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountAboveSepaLimit r ->
          CreateBoostResponse.newBuilder()
              .setAmountAboveSepaLimit(AmountAboveSepaLimitError.newBuilder()
                  .setMaximum(r.maximum().toString()).build()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.Success _ -> {
        final com.stripe.model.PaymentIntent paymentIntent = stripeManager.createPaymentIntent(
            request.getCurrency(), request.getAmount(), request.getLevel(),
            getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null))).join();
        yield CreateBoostResponse.newBuilder().setClientSecret(paymentIntent.getClientSecret()).build();
      }
    };
  }

  @Override
  public CreatePayPalBoostResponse createPayPalBoost(final CreatePayPalBoostRequest request) {

    final OneTimeDonationUtil.OneTimeDonationRequestValidationResult validationResult =
        OneTimeDonationUtil.validateOneTimeDonationRequest(
            request.getCurrency(),
            BigDecimal.valueOf(request.getAmount()),
            request.getLevel(),
            org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.PAYPAL,
            oneTimeDonationConfiguration,
            braintreeManager);

    return switch (validationResult) {
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedLevel _ ->
          CreatePayPalBoostResponse.newBuilder()
              .setUnsupportedLevel(FailedPrecondition.getDefaultInstance()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedCurrency _ ->
          CreatePayPalBoostResponse.newBuilder()
              .setUnsupportedCurrency(FailedPrecondition.getDefaultInstance()).build();
      case final OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountBelowMinimum r ->
          CreatePayPalBoostResponse.newBuilder()
              .setAmountBelowMinimum(AmountBelowMinimumError.newBuilder()
                  .setMinimum(r.minimum().toString()).build()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountAboveSepaLimit _ ->
          throw new IllegalStateException("SEPA limit should not trigger for PayPal");
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.Success _ -> {
        final List<Locale> acceptableLocales = RequestAttributesUtil.getAvailableAcceptedLocales();
        final OneTimeDonationUtil.LocalizedPayPalDonationLineItem localizedLineItem = OneTimeDonationUtil.localizePayPalDonationLineItem(
            payPalDonationsTranslator, acceptableLocales);
        final BraintreeManager.PayPalOneTimePaymentApprovalDetails approvalDetails =
            braintreeManager.createOneTimePayment(
                request.getCurrency().toUpperCase(Locale.ROOT), request.getAmount(),
                localizedLineItem.locale().toLanguageTag(), request.getReturnUrl(), request.getCancelUrl(),
                localizedLineItem.itemName()).join();
        yield CreatePayPalBoostResponse.newBuilder()
            .setResult(CreatePayPalBoostResponse.CreatePayPalBoostResult.newBuilder()
                .setApprovalUrl(approvalDetails.approvalUrl())
                .setPaymentId(approvalDetails.paymentId()).build()).build();
      }
    };
  }

  @Override
  public ConfirmPayPalBoostResponse confirmPayPalBoost(final ConfirmPayPalBoostRequest request)
      throws RateLimitExceededException {
    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.forDescriptor(RateLimiters.For.ONE_TIME_DONATION));

    final OneTimeDonationUtil.OneTimeDonationRequestValidationResult validationResult =
        OneTimeDonationUtil.validateOneTimeDonationRequest(
            request.getCurrency(),
            BigDecimal.valueOf(request.getAmount()),
            request.getLevel(),
            org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.PAYPAL,
            oneTimeDonationConfiguration,
            braintreeManager);

    return switch (validationResult) {
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedLevel _ ->
          ConfirmPayPalBoostResponse.newBuilder()
              .setUnsupportedLevel(FailedPrecondition.getDefaultInstance()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.UnsupportedCurrency _ ->
          ConfirmPayPalBoostResponse.newBuilder()
              .setUnsupportedCurrency(FailedPrecondition.getDefaultInstance()).build();
      case final OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountBelowMinimum r ->
          ConfirmPayPalBoostResponse.newBuilder()
              .setAmountBelowMinimum(AmountBelowMinimumError.newBuilder()
                  .setMinimum(r.minimum().toString()).build()).build();
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.AmountAboveSepaLimit _ ->
          throw new IllegalStateException("SEPA limit should not trigger for PayPal");
      case OneTimeDonationUtil.OneTimeDonationRequestValidationResult.Success _ -> {
        final BraintreeManager.PayPalChargeSuccessDetails chargeSuccessDetails =
            braintreeManager.captureOneTimePayment(
                request.getPayerId(), request.getPaymentId(), request.getPaymentToken(),
                request.getCurrency(), request.getAmount(), request.getLevel(),
                getClientPlatform(RequestAttributesUtil.getUserAgent().orElse(null))).join();
        oneTimeDonationsManager.putPaidAt(chargeSuccessDetails.paymentId(), clock.instant());
        yield ConfirmPayPalBoostResponse.newBuilder()
            .setResult(ConfirmPayPalBoostResponse.ConfirmPayPalBoostResult.newBuilder()
                .setPaymentId(chargeSuccessDetails.paymentId()).build()).build();
      }
    };
  }

  @Override
  public CreateBoostReceiptCredentialsResponse createBoostReceiptCredentials(
      final CreateBoostReceiptCredentialsRequest request) {

    final PaymentProvider processor;
    final Optional<PaymentDetails> maybePaymentDetails;
    switch (request.getProcessor()) {
      case PAYMENT_PROVIDER_STRIPE -> {
        processor = PaymentProvider.STRIPE;
        maybePaymentDetails = stripeManager.getPaymentDetails(request.getPaymentIntentId()).join();
      }
      case PAYMENT_PROVIDER_BRAINTREE -> {
        processor = PaymentProvider.BRAINTREE;
        maybePaymentDetails = braintreeManager.getPaymentDetails(request.getPaymentIntentId()).join();
      }
      default -> throw GrpcExceptions.fieldViolation("processor", "Unsupported payment processor");
    }

    if (maybePaymentDetails.isEmpty()) {
      return CreateBoostReceiptCredentialsResponse.newBuilder()
          .setPaymentNotFound(NotFound.getDefaultInstance()).build();
    }
    final PaymentDetails paymentDetails = maybePaymentDetails.get();
    if (paymentDetails.status() == PaymentStatus.PROCESSING) {
      return CreateBoostReceiptCredentialsResponse.newBuilder()
          .setPaymentStillProcessing(FailedPrecondition.getDefaultInstance()).build();
    }
    if (paymentDetails.status() != PaymentStatus.SUCCEEDED) {
      final PaymentRequired.Builder paymentRequiredBuilder = PaymentRequired.newBuilder();
      if (paymentDetails.chargeFailure() != null) {
        paymentRequiredBuilder.setChargeFailure(toChargeFailure(processor, paymentDetails.chargeFailure()));
      }
      return CreateBoostReceiptCredentialsResponse.newBuilder()
          .setPaymentRequired(paymentRequiredBuilder).build();
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

    try {
      issuedReceiptsManager.recordIssuance(
          paymentDetails.id(), processor, receiptCredentialRequest, clock.instant());
    } catch (final WriteConflictException e) {
      return CreateBoostReceiptCredentialsResponse.newBuilder()
          .setReceiptAlreadyIssued(FailedPrecondition.getDefaultInstance()).build();
    }

    final Instant paidAt = oneTimeDonationsManager.getPaidAt(paymentDetails.id(), paymentDetails.created());
    final Instant expiration = paidAt
        .plus(levelDetails.levelExpiration())
        .truncatedTo(ChronoUnit.DAYS)
        .plus(1, ChronoUnit.DAYS);

    final ReceiptCredentialResponse receiptCredentialResponse;
    try {
      receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
          receiptCredentialRequest, expiration.getEpochSecond(), levelDetails.level());
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

    return CreateBoostReceiptCredentialsResponse.newBuilder()
        .setResult(CreateBoostReceiptCredentialsResponse.CreateBoostReceiptCredentialsResult.newBuilder()
            .setReceiptCredentialResponse(ByteString.copyFrom(receiptCredentialResponse.serialize()))
            .build())
        .build();
  }

}
