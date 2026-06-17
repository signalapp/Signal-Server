/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import com.stripe.model.PaymentIntent;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.one_time_donations.ConfirmPayPalBoostRequest;
import org.signal.chat.one_time_donations.ConfirmPayPalBoostResponse;
import org.signal.chat.one_time_donations.CreateBoostReceiptCredentialsRequest;
import org.signal.chat.one_time_donations.CreateBoostReceiptCredentialsResponse;
import org.signal.chat.one_time_donations.CreateBoostRequest;
import org.signal.chat.one_time_donations.CreateBoostResponse;
import org.signal.chat.one_time_donations.CreatePayPalBoostRequest;
import org.signal.chat.one_time_donations.CreatePayPalBoostResponse;
import org.signal.chat.one_time_donations.OneTimeDonationsGrpc;
import org.signal.chat.subscriptions.PaymentMethod;
import org.signal.chat.subscriptions.PaymentProvider;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequestContext;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.DonationPermits;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.SubscriptionConfigTestHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

public class OneTimeDonationsGrpcServiceTest extends
    SimpleBaseGrpcTest<OneTimeDonationsGrpcService, OneTimeDonationsGrpc.OneTimeDonationsBlockingStub> {

  private final TestClock clock = TestClock.pinned(Instant.now());

  private final OneTimeDonationConfiguration oneTimeDonationConfiguration =
      SubscriptionConfigTestHelper.getOneTimeConfig();

  @Mock
  private DonationPermits donationPermits;

  private static final ServerSecretParams DONATION_PERMITS_SECRET_PARAMS = ServerSecretParams.generate();
  private DonationPermitsManager donationPermitsManager;

  @Mock
  private StripeManager stripeManager;

  @Mock
  private BraintreeManager braintreeManager;

  @Mock
  private PayPalDonationsTranslator payPalDonationsTranslator;

  @Mock
  private OneTimeDonationsManager oneTimeDonationsManager;

  @Mock
  private IssuedReceiptsManager issuedReceiptsManager;

  @Mock
  private ServerZkReceiptOperations zkReceiptOperations;

  @Mock
  private RateLimiters rateLimiters;

  @Mock
  private RateLimiter rateLimiter;

  @Override
  protected OneTimeDonationsGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "en-us"));

    donationPermitsManager = new DonationPermitsManager(donationPermits, DONATION_PERMITS_SECRET_PARAMS, clock);

    // spendIds are spend-once
    final Set<String> spent = new HashSet<>();
    when(donationPermits.spend(any(byte[].class), any(Instant.class)))
        .thenAnswer(answer -> spent.add(new String(answer.getArgument(0, byte[].class))));

    when(rateLimiters.forDescriptor(RateLimiters.For.ONE_TIME_DONATION)).thenReturn(rateLimiter);

    return new OneTimeDonationsGrpcService(
        oneTimeDonationConfiguration,
        stripeManager,
        braintreeManager,
        payPalDonationsTranslator,
        oneTimeDonationsManager,
        issuedReceiptsManager,
        zkReceiptOperations,
        clock,
        rateLimiters,
        donationPermitsManager);
  }

  @Test
  void createBoost() {
    final PaymentIntent paymentIntent = mock(PaymentIntent.class);
    when(paymentIntent.getClientSecret()).thenReturn("test-client-secret");
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.CARD))
        .thenReturn(Set.of("usd"));
    when(stripeManager.createPaymentIntent(any(), anyLong(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(paymentIntent));

    final CreateBoostResponse response = unauthenticatedServiceStub().createBoost(
        CreateBoostRequest.newBuilder()
            .setCurrency("usd")
            .setAmount(500)
            .setLevel(1)
            .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_CARD)
            .setDonationPermit(ByteString.copyFrom(getDonationPermit().serialize()))
            .build());

    assertEquals(CreateBoostResponse.ResponseCase.CLIENT_SECRET, response.getResponseCase());
    assertEquals("test-client-secret", response.getClientSecret());
  }

  @Test
  void createBoostPermitAlreadySpent() {
    when(donationPermits.spend(any(byte[].class), any(Instant.class))).thenReturn(false);

    final CreateBoostResponse response = unauthenticatedServiceStub().createBoost(
        CreateBoostRequest.newBuilder()
            .setCurrency("usd")
            .setAmount(500)
            .setLevel(1)
            .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_CARD)
            .setDonationPermit(ByteString.copyFrom(getDonationPermit().serialize()))
            .build());

    assertEquals(CreateBoostResponse.ResponseCase.PERMIT_REJECTED, response.getResponseCase());
  }

  @ParameterizedTest
  @MethodSource
  void createBoostValidationErrors(
      final String currency,
      final long amount,
      final PaymentMethod paymentMethod,
      final CreateBoostResponse.ResponseCase expectedResponseCase) {
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.CARD))
        .thenReturn(Set.of("usd"));
    when(stripeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.SEPA_DEBIT))
        .thenReturn(Set.of("eur"));

    final CreateBoostResponse response = unauthenticatedServiceStub().createBoost(
        CreateBoostRequest.newBuilder()
            .setCurrency(currency)
            .setAmount(amount)
            .setLevel(1)
            .setPaymentMethod(paymentMethod)
            .setDonationPermit(ByteString.copyFrom(getDonationPermit().serialize()))
            .build());

    assertEquals(expectedResponseCase, response.getResponseCase());
  }

  static Stream<Arguments> createBoostValidationErrors() {
    return Stream.of(
        Arguments.of("usd", 249L, PaymentMethod.PAYMENT_METHOD_CARD,
            CreateBoostResponse.ResponseCase.AMOUNT_BELOW_MINIMUM),
        Arguments.of("eur", 1000001L, PaymentMethod.PAYMENT_METHOD_SEPA_DEBIT,
            CreateBoostResponse.ResponseCase.AMOUNT_ABOVE_SEPA_LIMIT),
        // USD is not supported for SEPA_DEBIT
        Arguments.of("usd", 3000L, PaymentMethod.PAYMENT_METHOD_SEPA_DEBIT,
            CreateBoostResponse.ResponseCase.UNSUPPORTED_CURRENCY)
    );
  }

  @Test
  void createPayPalBoost() {
    final BraintreeManager.PayPalOneTimePaymentApprovalDetails approvalDetails =
        mock(BraintreeManager.PayPalOneTimePaymentApprovalDetails.class);
    when(approvalDetails.approvalUrl()).thenReturn("test-approval-url");
    when(approvalDetails.paymentId()).thenReturn("test-id");
    when(braintreeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd"));
    when(payPalDonationsTranslator.translate(any(), any()))
        .thenReturn("Donation to Signal Technology Foundation");
    when(braintreeManager.createOneTimePayment(anyString(), anyLong(), anyString(), anyString(), anyString(),
        anyString()))
        .thenReturn(CompletableFuture.completedFuture(approvalDetails));

    final CreatePayPalBoostResponse response = unauthenticatedServiceStub().createPayPalBoost(
        CreatePayPalBoostRequest.newBuilder()
            .setCurrency("usd")
            .setAmount(300)
            .setLevel(1)
            .setReturnUrl("returnUrl")
            .setCancelUrl("cancelUrl")
            .build());

    assertEquals(CreatePayPalBoostResponse.ResponseCase.RESULT, response.getResponseCase());
    assertEquals("test-approval-url", response.getResult().getApprovalUrl());
    assertEquals("test-id", response.getResult().getPaymentId());
  }

  @Test
  void confirmPayPalBoost() {
    when(braintreeManager.getSupportedCurrenciesForPaymentMethod(
        org.whispersystems.textsecuregcm.subscriptions.PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd"));
    when(braintreeManager.captureOneTimePayment(anyString(), anyString(), anyString(), anyString(), anyLong(),
        anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            new BraintreeManager.PayPalChargeSuccessDetails("test-id")));

    final ConfirmPayPalBoostResponse response = unauthenticatedServiceStub().confirmPayPalBoost(
        ConfirmPayPalBoostRequest.newBuilder()
            .setCurrency("usd")
            .setAmount(300)
            .setLevel(1)
            .setPayerId("test-payer-id")
            .setPaymentId("test-payment-id")
            .setPaymentToken("test-payment-token")
            .build());

    assertEquals(ConfirmPayPalBoostResponse.ResponseCase.RESULT, response.getResponseCase());
    assertEquals("test-id", response.getResult().getPaymentId());
  }

  @ParameterizedTest
  @MethodSource
  void createBoostReceiptCredentialsPaymentRequired(
      @Nullable final ChargeFailure chargeFailure,
      final boolean expectChargeFailure) {
    when(stripeManager.getPaymentDetails(any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new PaymentDetails("id", Collections.emptyMap(), PaymentStatus.FAILED,
            clock.instant(), chargeFailure))));

    final CreateBoostReceiptCredentialsResponse response =
        unauthenticatedServiceStub().createBoostReceiptCredentials(
            CreateBoostReceiptCredentialsRequest.newBuilder()
                .setPaymentIntentId("test-payment-intent-id")
                .setReceiptCredentialRequest(ByteString.copyFromUtf8("abcd"))
                .setProcessor(PaymentProvider.PAYMENT_PROVIDER_STRIPE)
                .build());

    assertEquals(CreateBoostReceiptCredentialsResponse.ResponseCase.PAYMENT_REQUIRED,
        response.getResponseCase());
    if (expectChargeFailure) {
      assertEquals("generic_decline", response.getPaymentRequired().getChargeFailure().getCode());
    } else {
      assertFalse(response.getPaymentRequired().hasChargeFailure());
    }
  }

  static Stream<Arguments> createBoostReceiptCredentialsPaymentRequired() {
    return Stream.of(
        Arguments.of(new ChargeFailure("generic_decline", "some failure message", null, null, null), true),
        Arguments.of(null, false)
    );
  }

  @Test
  void createBoostReceiptCredentialsAlreadyRedeemed() throws Exception {
    final ReceiptCredentialRequest receiptCredentialRequest = new ClientZkReceiptOperations(
        ServerSecretParams.generate().getPublicParams()).createReceiptCredentialRequestContext(
        new ReceiptSerial(new byte[ReceiptSerial.SIZE])).getRequest();

    when(stripeManager.getPaymentDetails(any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new PaymentDetails("id", Collections.emptyMap(), PaymentStatus.SUCCEEDED,
            clock.instant(), null))));
    doThrow(WriteConflictException.class).when(issuedReceiptsManager)
        .recordIssuance(any(), any(), any(), any());

    final CreateBoostReceiptCredentialsResponse response =
        unauthenticatedServiceStub().createBoostReceiptCredentials(
            CreateBoostReceiptCredentialsRequest.newBuilder()
                .setPaymentIntentId("test-payment-intent-id")
                .setReceiptCredentialRequest(ByteString.copyFrom(receiptCredentialRequest.serialize()))
                .setProcessor(PaymentProvider.PAYMENT_PROVIDER_STRIPE)
                .build());

    assertEquals(CreateBoostReceiptCredentialsResponse.ResponseCase.RECEIPT_ALREADY_ISSUED,
        response.getResponseCase());
  }

  private DonationPermit getDonationPermit() {
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(1);
    final DonationPermitRequest permitRequest = context.request();
    final DonationPermitResponse permitResponse = donationPermitsManager.issue(permitRequest);
    try {
      final List<DonationPermit> permits = context.receive(
          permitResponse, DONATION_PERMITS_SECRET_PARAMS.getPublicParams(), clock.instant());
      return permits.getFirst();
    } catch (final VerificationFailedException e) {
      throw new AssertionError("The permit was correctly requested and issued in this method", e);
    }
  }
}
