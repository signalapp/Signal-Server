/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.purchase.CreatePayPalPaymentMethodRequest;
import org.signal.chat.purchase.CreatePayPalPaymentMethodResponse;
import org.signal.chat.purchase.CreatePaymentMethodRequest;
import org.signal.chat.purchase.CreatePaymentMethodResponse;
import org.signal.chat.purchase.DeleteSubscriberRequest;
import org.signal.chat.purchase.DeleteSubscriberResponse;
import org.signal.chat.purchase.GetBankMandateRequest;
import org.signal.chat.purchase.GetBankMandateResponse;
import org.signal.chat.purchase.GetReceiptCredentialRequest;
import org.signal.chat.purchase.GetReceiptCredentialResponse;
import org.signal.chat.purchase.GetSubscriptionInformationRequest;
import org.signal.chat.purchase.GetSubscriptionInformationResponse;
import org.signal.chat.purchase.PaymentMethod;
import org.signal.chat.purchase.SetDefaultPaymentMethodRequest;
import org.signal.chat.purchase.SetDefaultPaymentMethodResponse;
import org.signal.chat.purchase.SetIapSubscriptionRequest;
import org.signal.chat.purchase.SetIapSubscriptionResponse;
import org.signal.chat.purchase.SetSubscriptionLevelRequest;
import org.signal.chat.purchase.SetSubscriptionLevelResponse;
import org.signal.chat.purchase.SubscriptionsGrpc;
import org.signal.chat.purchase.UpdateSubscriberRequest;
import org.signal.chat.purchase.UpdateSubscriberResponse;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequestContext;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.DonationPermits;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.purchases.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.purchases.BankMandateTranslator;
import org.whispersystems.textsecuregcm.purchases.BankTransferType;
import org.whispersystems.textsecuregcm.purchases.BraintreeManager;
import org.whispersystems.textsecuregcm.purchases.ChargeFailure;
import org.whispersystems.textsecuregcm.purchases.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.purchases.PaymentProvider;
import org.whispersystems.textsecuregcm.purchases.ProcessorCustomer;
import org.whispersystems.textsecuregcm.purchases.StripeManager;
import org.whispersystems.textsecuregcm.purchases.SubscriberIdCreationNotPermittedException;
import org.whispersystems.textsecuregcm.purchases.PurchaseException;
import org.whispersystems.textsecuregcm.purchases.PurchaseForbiddenException;
import org.whispersystems.textsecuregcm.purchases.SubscriptionInformation;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidArgumentsException;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidIdempotencyKeyException;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidLevelException;
import org.whispersystems.textsecuregcm.purchases.PurchaseNotFoundException;
import org.whispersystems.textsecuregcm.purchases.SubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.purchases.PurchasePaymentRequiredException;
import org.whispersystems.textsecuregcm.purchases.PurchasePaymentRequiresActionException;
import org.whispersystems.textsecuregcm.purchases.SubscriptionPrice;
import org.whispersystems.textsecuregcm.purchases.PurchaseProcessorConflictException;
import org.whispersystems.textsecuregcm.purchases.PurchaseProcessorException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptRequestedForOpenPaymentException;
import org.whispersystems.textsecuregcm.purchases.SubscriptionStatus;
import org.whispersystems.textsecuregcm.tests.util.SubscriptionConfigTestHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class SubscriptionsGrpcServiceTest extends
    SimpleBaseGrpcTest<SubscriptionsGrpcService, SubscriptionsGrpc.SubscriptionsBlockingStub> {

  private final TestClock clock = TestClock.pinned(Instant.now());

  private final SubscriptionConfiguration subscriptionConfiguration =
      SubscriptionConfigTestHelper.getSubscriptionConfig();

  @Mock
  private DonationPermits donationPermits;

  private static final ServerSecretParams DONATION_PERMITS_SECRET_PARAMS = ServerSecretParams.generate();
  private DonationPermitsManager donationPermitsManager;

  @Mock
  private SubscriptionManager subscriptionManager;

  @Mock
  private StripeManager stripeManager;

  @Mock
  private BraintreeManager braintreeManager;

  @Mock
  private GooglePlayBillingManager googlePlayBillingManager;

  @Mock
  private AppleAppStoreManager appleAppStoreManager;

  @Mock
  private BankMandateTranslator bankMandateTranslator;

  private static final ByteString SUBSCRIBER_ID = ByteString.copyFrom(TestRandomUtil.nextBytes(32));
  private static final long LEVEL = 5L;
  private static final String CURRENCY = "usd";

  @Override
  protected SubscriptionsGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "en-us"));

     donationPermitsManager = new DonationPermitsManager(donationPermits, DONATION_PERMITS_SECRET_PARAMS, clock);

    // spendIds are spend-once
    final Set<String> spent = new HashSet<>();
    when(donationPermits.spend(any(byte[].class), any(Instant.class)))
        .thenAnswer(answer -> spent.add(new String(answer.getArgument(0, byte[].class))));

    return new SubscriptionsGrpcService(clock, subscriptionConfiguration, subscriptionManager, donationPermitsManager,
        stripeManager, braintreeManager, googlePlayBillingManager, appleAppStoreManager, bankMandateTranslator);
  }

  @Test
  void updateSubscriber() {
    final UpdateSubscriberResponse response = unauthenticatedServiceStub().updateSubscriber(
        UpdateSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(UpdateSubscriberResponse.ResponseCase.SUCCESS, response.getResponseCase());
  }

  @Test
  void updateSubscriberCreationNotPermittedPermitRejected() throws PurchaseException {
    doThrow(new SubscriberIdCreationNotPermittedException())
        .when(subscriptionManager).updateSubscriber(any(), eq(false));

    final DonationPermit donationPermit = getDonationPermit();
    clock.pin(clock.instant().plus(Duration.ofDays(14)));

    final UpdateSubscriberResponse response = unauthenticatedServiceStub().updateSubscriber(
        UpdateSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setDonationPermit(ByteString.copyFrom(donationPermit.serialize()))
            .build());

    assertTrue(response.hasPermitRejected());
  }

  @Test
  void updateSubscriberCreationNotPermittedMissingPermit() throws PurchaseException {
    doThrow(new SubscriberIdCreationNotPermittedException())
        .when(subscriptionManager).updateSubscriber(any(), eq(false));
    GrpcTestUtils.assertStatusInvalidArgument(() -> unauthenticatedServiceStub().updateSubscriber(
        UpdateSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build()));
  }

  @Test
  void updateSubscriberIdMismatch() throws PurchaseException {
    doThrow(new PurchaseForbiddenException("subscriberId mismatch"))
        .when(subscriptionManager).updateSubscriber(any(), anyBoolean());
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().updateSubscriber(
        UpdateSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build()));
  }

  @Test
  void deleteSubscriber() {
    final DeleteSubscriberResponse response = unauthenticatedServiceStub().deleteSubscriber(
        DeleteSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(DeleteSubscriberResponse.ResponseCase.SUCCESS, response.getResponseCase());
  }

  @Test
  void deleteSubscriberNotFound()
      throws PurchaseNotFoundException, PurchaseInvalidArgumentsException, RateLimitExceededException {
    doThrow(new PurchaseNotFoundException())
        .when(subscriptionManager).deleteSubscriber(any());
    final DeleteSubscriberResponse response = unauthenticatedServiceStub().deleteSubscriber(
        DeleteSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(DeleteSubscriberResponse.ResponseCase.SUBSCRIBER_NOT_FOUND, response.getResponseCase());
  }

  @Test
  void deleteSubscriberCannotCancel()
      throws PurchaseNotFoundException, PurchaseInvalidArgumentsException, RateLimitExceededException {
    doThrow(new PurchaseInvalidArgumentsException("cannot cancel subscription"))
        .when(subscriptionManager).deleteSubscriber(any());
    final DeleteSubscriberResponse response = unauthenticatedServiceStub().deleteSubscriber(
        DeleteSubscriberRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(DeleteSubscriberResponse.ResponseCase.CANNOT_CANCEL_SUBSCRIPTION, response.getResponseCase());
  }

  @ParameterizedTest
  @EnumSource(value = PaymentMethod.class, names = {"PAYMENT_METHOD_CARD", "PAYMENT_METHOD_SEPA_DEBIT",
      "PAYMENT_METHOD_IDEAL"})
  void createPaymentMethod(final PaymentMethod paymentMethod)
      throws PurchaseForbiddenException, PurchaseNotFoundException, PurchaseProcessorConflictException {
    when(subscriptionManager.addPaymentMethodToCustomer(any(), any(), any(), any())).thenReturn("test-client-secret");
    when(stripeManager.getProvider()).thenReturn(PaymentProvider.STRIPE);

    final CreatePaymentMethodRequest.Builder builder = CreatePaymentMethodRequest.newBuilder()
        .setSubscriberId(SUBSCRIBER_ID)
        .setDonationPermit(ByteString.copyFrom(getDonationPermit().serialize()))
        .setPaymentMethod(paymentMethod);
    final CreatePaymentMethodResponse response = unauthenticatedServiceStub().createPaymentMethod(builder.build());
    assertEquals(CreatePaymentMethodResponse.ResponseCase.RESULT, response.getResponseCase());
    // Currently, only stripe is chosen as the payment provider for all supported payment methods
    assertEquals(org.signal.chat.purchase.PaymentProvider.PAYMENT_PROVIDER_STRIPE,
        response.getResult().getPaymentProvider());
    assertEquals("test-client-secret", response.getResult().getClientSecret());
  }

  @Test
  void createPaymentMethodUnsupportedPaymentMethod() {
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> unauthenticatedServiceStub().createPaymentMethod(
            CreatePaymentMethodRequest.newBuilder()
                .setSubscriberId(SUBSCRIBER_ID)
                .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_PAYPAL)
                .build()));
  }

  @Test
  void createPaymentMethodInvalidDonationPermit() {
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> unauthenticatedServiceStub().createPaymentMethod(
            CreatePaymentMethodRequest.newBuilder()
                .setSubscriberId(SUBSCRIBER_ID)
                .setDonationPermit(ByteString.copyFrom(new byte[]{1}))
                .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_CARD)
                .build()));
  }

  @Test
  void createPaymentMethodExpiredDonationPermit() {
    when(stripeManager.getProvider()).thenReturn(PaymentProvider.STRIPE);

    final DonationPermit donationPermit = getDonationPermit();
    clock.pin(clock.instant().plus(Duration.ofDays(30)));

    final CreatePaymentMethodResponse createPaymentMethodResponse = unauthenticatedServiceStub().createPaymentMethod(
        CreatePaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setDonationPermit(ByteString.copyFrom(donationPermit.serialize()))
            .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_CARD)
            .build());

    assertTrue(createPaymentMethodResponse.hasPermitRejected());
  }

  @Test
  void createPaymentMethodProcessorConflict()
      throws PurchaseForbiddenException, PurchaseNotFoundException, PurchaseProcessorConflictException {
    doThrow(new PurchaseProcessorConflictException())
        .when(subscriptionManager).addPaymentMethodToCustomer(any(), any(), any(), any());
    final CreatePaymentMethodResponse response = unauthenticatedServiceStub().createPaymentMethod(
        CreatePaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setDonationPermit(ByteString.copyFrom(getDonationPermit().serialize()))
            .setPaymentMethod(PaymentMethod.PAYMENT_METHOD_CARD)
            .build());
    assertEquals(CreatePaymentMethodResponse.ResponseCase.SUBSCRIPTION_PROCESSOR_CONFLICT, response.getResponseCase());
  }

  @Test
  void createPayPalPaymentMethod()
      throws PurchaseForbiddenException, PurchaseNotFoundException, PurchaseProcessorConflictException {
    final BraintreeManager.PayPalBillingAgreementApprovalDetails details =
        new BraintreeManager.PayPalBillingAgreementApprovalDetails("https://fake-approval", "test-billing-token");
    when(subscriptionManager.addPaymentMethodToCustomer(any(), any(), any(), any()))
        .thenReturn(details);
    final CreatePayPalPaymentMethodResponse response = unauthenticatedServiceStub().createPayPalPaymentMethod(
        CreatePayPalPaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setReturnUrl("https://fake-return")
            .setCancelUrl("https://fake-cancel")
            .build());
    assertEquals(CreatePayPalPaymentMethodResponse.ResponseCase.RESULT, response.getResponseCase());
    assertEquals("https://fake-approval", response.getResult().getApprovalUrl());
    assertEquals("test-billing-token", response.getResult().getToken());
  }

  @Test
  void createPayPalPaymentMethodProcessorConflict()
      throws PurchaseForbiddenException, PurchaseNotFoundException, PurchaseProcessorConflictException {
    doThrow(new PurchaseProcessorConflictException())
        .when(subscriptionManager).addPaymentMethodToCustomer(any(), any(), any(), any());
    final CreatePayPalPaymentMethodResponse response = unauthenticatedServiceStub().createPayPalPaymentMethod(
        CreatePayPalPaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setReturnUrl("https://fake-return")
            .setCancelUrl("https://fake-cancel")
            .build());
    assertEquals(CreatePayPalPaymentMethodResponse.ResponseCase.SUBSCRIPTION_PROCESSOR_CONFLICT,
        response.getResponseCase());
  }

  @ParameterizedTest
  @EnumSource(value = SetDefaultPaymentMethodRequest.RequestCase.class, names = {"STRIPE", "BRAINTREE", "SEPA"})
  void setDefaultPaymentMethod(final SetDefaultPaymentMethodRequest.RequestCase requestCase)
      throws PurchaseNotFoundException, PurchaseForbiddenException, IOException, PurchaseProcessorConflictException {
    final PaymentProvider provider = requestCase == SetDefaultPaymentMethodRequest.RequestCase.BRAINTREE
        ? PaymentProvider.BRAINTREE : PaymentProvider.STRIPE;
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(Optional.of(new ProcessorCustomer("test-customer", provider)));
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
    if (requestCase == SetDefaultPaymentMethodRequest.RequestCase.SEPA) {
      when(stripeManager.getGeneratedSepaIdFromSetupIntent(any()))
          .thenReturn("sepa-id");
    }
    final SetDefaultPaymentMethodRequest.Builder builder = SetDefaultPaymentMethodRequest.newBuilder()
        .setSubscriberId(SUBSCRIBER_ID);
    switch (requestCase) {
      case STRIPE -> builder.setStripe(SetDefaultPaymentMethodRequest.StripePaymentMethod.newBuilder()
          .setPaymentMethodToken("test-stripe-token").build());
      case BRAINTREE -> builder.setBraintree(SetDefaultPaymentMethodRequest.BraintreePaymentMethod.newBuilder()
          .setPaymentMethodToken("test-braintree-token").build());
      case SEPA -> builder.setSepa(SetDefaultPaymentMethodRequest.SepaPaymentMethod.newBuilder()
          .setSetupIntentId("test-setup-intent-id").build());
      default -> throw new IllegalArgumentException("Unexpected case: " + requestCase);
    }
    final SetDefaultPaymentMethodResponse response =
        unauthenticatedServiceStub().setDefaultPaymentMethod(builder.build());
    assertEquals(SetDefaultPaymentMethodResponse.ResponseCase.SUCCESS, response.getResponseCase());
  }

  @Test
  void setDefaultPaymentMethodNoRequest() {
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> unauthenticatedServiceStub().setDefaultPaymentMethod(
            SetDefaultPaymentMethodRequest.newBuilder()
                .setSubscriberId(SUBSCRIBER_ID)
                .build()));
  }

  @Test
  void setDefaultPaymentMethodSubscriptionNotFound()
      throws PurchaseNotFoundException, PurchaseForbiddenException {
    doThrow(new PurchaseNotFoundException())
        .when(subscriptionManager).getSubscriber(any());
    final SetDefaultPaymentMethodResponse response = unauthenticatedServiceStub().setDefaultPaymentMethod(
        SetDefaultPaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setStripe(SetDefaultPaymentMethodRequest.StripePaymentMethod.newBuilder()
                .setPaymentMethodToken("test-token").build())
            .build());
    assertEquals(SetDefaultPaymentMethodResponse.ResponseCase.SUBSCRIBER_NOT_FOUND, response.getResponseCase());
  }

  @Test
  void setDefaultPaymentMethodNoCustomer() throws PurchaseNotFoundException, PurchaseForbiddenException {
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(Optional.empty());
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
    final SetDefaultPaymentMethodResponse response = unauthenticatedServiceStub().setDefaultPaymentMethod(
        SetDefaultPaymentMethodRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setStripe(SetDefaultPaymentMethodRequest.StripePaymentMethod.newBuilder()
                .setPaymentMethodToken("test-token").build())
            .build());
    assertEquals(SetDefaultPaymentMethodResponse.ResponseCase.PAYMENT_METHOD_NOT_SET_UP, response.getResponseCase());
  }

  private void mockValidSubscription() throws PurchaseNotFoundException, PurchaseForbiddenException {
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(
        Optional.of(new ProcessorCustomer("test-customer", PaymentProvider.STRIPE)));
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
  }

  @Test
  void setSubscriptionLevel() throws PurchaseException {
    mockValidSubscription();
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL)
            .setCurrency(CURRENCY)
            .setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.SUCCESS, response.getResponseCase());
    assertEquals(LEVEL, response.getSuccess().getLevel());
  }

  @Test
  void setSubscriptionLevelNotFound() throws PurchaseNotFoundException, PurchaseForbiddenException {
    doThrow(new PurchaseNotFoundException())
        .when(subscriptionManager).getSubscriber(any());
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL).setCurrency(CURRENCY).setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.SUBSCRIBER_NOT_FOUND, response.getResponseCase());
  }

  @Test
  void setSubscriptionLevelNoCustomer() throws PurchaseNotFoundException, PurchaseForbiddenException {
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(Optional.empty());
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL).setCurrency(CURRENCY).setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.PAYMENT_METHOD_NOT_SET_UP, response.getResponseCase());
  }

  @Test
  void setSubscriptionLevelUnsupportedLevel() throws PurchaseNotFoundException, PurchaseForbiddenException {
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(
        Optional.of(new ProcessorCustomer("test-customer", PaymentProvider.STRIPE)));
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(999L).setCurrency(CURRENCY).setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.UNSUPPORTED_LEVEL, response.getResponseCase());
  }

  @Test
  void setSubscriptionLevelUnsupportedCurrency() throws PurchaseNotFoundException, PurchaseForbiddenException {
    final Subscriptions.Record record = mock(Subscriptions.Record.class);
    when(record.getProcessorCustomer()).thenReturn(
        Optional.of(new ProcessorCustomer("test-customer", PaymentProvider.STRIPE)));
    when(subscriptionManager.getSubscriber(any())).thenReturn(record);
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL).setCurrency("xyz").setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.UNSUPPORTED_CURRENCY, response.getResponseCase());
  }

  @ParameterizedTest
  @MethodSource
  void setSubscriptionLevelUpdateExceptions(final PurchaseException exception,
      final SetSubscriptionLevelResponse.ResponseCase expectedCase)
      throws PurchaseException {
    mockValidSubscription();
    doThrow(exception).when(subscriptionManager).updateSubscriptionLevelForCustomer(
        any(), any(), any(), anyLong(), any(), any(), any(), any());
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL).setCurrency(CURRENCY).setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(expectedCase, response.getResponseCase());
  }

  static Stream<Arguments> setSubscriptionLevelUpdateExceptions() {
    return Stream.of(
        Arguments.of(new PurchaseInvalidIdempotencyKeyException("bad key"),
            SetSubscriptionLevelResponse.ResponseCase.INVALID_IDEMPOTENCY_KEY),
        Arguments.of(new PurchasePaymentRequiresActionException(),
            SetSubscriptionLevelResponse.ResponseCase.PAYMENT_REQUIRES_ACTION),
        Arguments.of(new PurchaseInvalidLevelException(),
            SetSubscriptionLevelResponse.ResponseCase.INVALID_LEVEL_TRANSITION),
        Arguments.of(new PurchaseProcessorConflictException(),
            SetSubscriptionLevelResponse.ResponseCase.SUBSCRIPTION_PROCESSOR_CONFLICT)
    );
  }

  @Test
  void setSubscriptionLevelChargeFailure()
      throws PurchaseException {
    mockValidSubscription();
    doThrow(new PurchaseProcessorException(PaymentProvider.STRIPE,
        new ChargeFailure("card_declined", "Insufficient funds", null, null, null)))
        .when(subscriptionManager).updateSubscriptionLevelForCustomer(
            any(), any(), any(), anyLong(), any(), any(), any(), any());
    final SetSubscriptionLevelResponse response = unauthenticatedServiceStub().setSubscriptionLevel(
        SetSubscriptionLevelRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setLevel(LEVEL).setCurrency(CURRENCY).setIdempotencyKey("test-idempotency-key")
            .build());
    assertEquals(SetSubscriptionLevelResponse.ResponseCase.CHARGE_FAILURE, response.getResponseCase());
    assertEquals(org.signal.chat.purchase.PaymentProvider.PAYMENT_PROVIDER_STRIPE,
        response.getChargeFailure().getProcessor());
    assertEquals("card_declined", response.getChargeFailure().getCode());
    assertEquals("Insufficient funds", response.getChargeFailure().getMessage());
  }

  @Test
  void getSubscriptionInformation()
      throws PurchaseNotFoundException, PurchaseForbiddenException, RateLimitExceededException {
    final SubscriptionInformation info = new SubscriptionInformation(
        new SubscriptionPrice(CURRENCY, 500),
        LEVEL,
        Instant.ofEpochSecond(100),
        Instant.ofEpochSecond(1000),
        true,
        false,
        SubscriptionStatus.ACTIVE,
        PaymentProvider.STRIPE,
        org.whispersystems.textsecuregcm.purchases.PaymentMethod.CARD,
        false,
        null);
    when(subscriptionManager.getSubscriptionInformation(any())).thenReturn(Optional.of(info));
    final GetSubscriptionInformationResponse response = unauthenticatedServiceStub().getSubscriptionInformation(
        GetSubscriptionInformationRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(GetSubscriptionInformationResponse.ResponseCase.SUCCESS, response.getResponseCase());
    assertEquals(LEVEL, response.getSuccess().getLevel());
    assertEquals(CURRENCY, response.getSuccess().getCurrency());
    assertEquals(500L, response.getSuccess().getAmount());
    assertEquals(1000L, response.getSuccess().getEndOfCurrentPeriod());
    assertTrue(response.getSuccess().getActive());
    assertFalse(response.getSuccess().getCancelAtPeriodEnd());
    assertEquals(org.signal.chat.purchase.PaymentProvider.PAYMENT_PROVIDER_STRIPE,
        response.getSuccess().getProcessor());
    assertEquals(PaymentMethod.PAYMENT_METHOD_CARD, response.getSuccess().getPaymentMethod());
  }

  @Test
  void getSubscriptionInformationNoSubscription()
      throws PurchaseNotFoundException, PurchaseForbiddenException, RateLimitExceededException {
    when(subscriptionManager.getSubscriptionInformation(any())).thenReturn(Optional.empty());
    final GetSubscriptionInformationResponse response = unauthenticatedServiceStub().getSubscriptionInformation(
        GetSubscriptionInformationRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .build());
    assertEquals(GetSubscriptionInformationResponse.ResponseCase.NO_SUBSCRIPTION, response.getResponseCase());
  }

  @Test
  void getReceiptCredential() throws Exception {
    final ReceiptCredentialResponse receiptCredentialResponse = mock(ReceiptCredentialResponse.class);
    final byte[] responseBytes = TestRandomUtil.nextBytes(16);
    when(receiptCredentialResponse.serialize()).thenReturn(responseBytes);
    when(subscriptionManager.createReceiptCredential(any(), any(), any(), any()))
        .thenReturn(new SubscriptionManager.ReceiptResult(receiptCredentialResponse, new SubscriptionPaymentProcessor.ReceiptItem("test-item-id", null, 5), PaymentProvider.STRIPE));
    final GetReceiptCredentialResponse response = unauthenticatedServiceStub().getReceiptCredential(
        GetReceiptCredentialRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setReceiptCredentialRequest(ByteString.copyFrom(TestRandomUtil.nextBytes(97)))
            .build());
    assertEquals(GetReceiptCredentialResponse.ResponseCase.SUCCESS, response.getResponseCase());
    assertArrayEquals(responseBytes, response.getSuccess().getReceiptCredentialResponse().toByteArray());
  }

  static Stream<Arguments> getReceiptCredentialExceptions() {
    return Stream.of(
        Arguments.of(new PurchaseReceiptRequestedForOpenPaymentException(),
            GetReceiptCredentialResponse.ResponseCase.NO_PAID_INVOICE),
        Arguments.of(new PurchasePaymentRequiredException(PaymentProvider.STRIPE),
            GetReceiptCredentialResponse.ResponseCase.PAYMENT_REQUIRED),
        Arguments.of(new PurchaseReceiptAlreadyRedeemedException(),
            GetReceiptCredentialResponse.ResponseCase.ALREADY_REDEEMED)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getReceiptCredentialExceptions(final PurchaseException exception,
      final GetReceiptCredentialResponse.ResponseCase expectedCase) throws Exception {
    doThrow(exception).when(subscriptionManager).createReceiptCredential(any(), any(), any(), any());
    final GetReceiptCredentialResponse response = unauthenticatedServiceStub().getReceiptCredential(
        GetReceiptCredentialRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setReceiptCredentialRequest(ByteString.copyFrom(TestRandomUtil.nextBytes(97)))
            .build());
    assertEquals(expectedCase, response.getResponseCase());
  }

  @Test
  void getReceiptCredentialChargeFailure() throws Exception {
    doThrow(new PurchasePaymentRequiredException(PaymentProvider.STRIPE,
        new ChargeFailure("card_declined", "Insufficient funds", null, null, null)))
        .when(subscriptionManager).createReceiptCredential(any(), any(), any(), any());
    final GetReceiptCredentialResponse response = unauthenticatedServiceStub().getReceiptCredential(
        GetReceiptCredentialRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setReceiptCredentialRequest(ByteString.copyFrom(TestRandomUtil.nextBytes(97)))
            .build());
    assertEquals(GetReceiptCredentialResponse.ResponseCase.PAYMENT_REQUIRED, response.getResponseCase());
    assertEquals(org.signal.chat.purchase.PaymentProvider.PAYMENT_PROVIDER_STRIPE,
        response.getPaymentRequired().getChargeFailure().getProcessor());
    assertEquals("card_declined", response.getPaymentRequired().getChargeFailure().getCode());
  }

  @ParameterizedTest
  @EnumSource(value = SetIapSubscriptionRequest.RequestCase.class, names = {"APP_STORE", "PLAY_BILLING"})
  void setIapSubscription(final SetIapSubscriptionRequest.RequestCase requestCase)
      throws PurchaseException, RateLimitExceededException {
    when(subscriptionManager.updateAppStoreTransactionId(any(), any(), any())).thenReturn(LEVEL);
    when(subscriptionManager.updatePlayBillingPurchaseToken(any(), any(), any())).thenReturn(LEVEL);
    final SetIapSubscriptionRequest.Builder builder = SetIapSubscriptionRequest.newBuilder()
        .setSubscriberId(SUBSCRIBER_ID);
    switch (requestCase) {
      case APP_STORE -> builder.setAppStore(SetIapSubscriptionRequest.AppStorePurchase.newBuilder()
          .setOriginalTransactionId("test-transaction-id").build());
      case PLAY_BILLING -> builder.setPlayBilling(SetIapSubscriptionRequest.PlayBillingPurchase.newBuilder()
          .setPurchaseToken("test-purchase-token").build());
      default -> throw new IllegalArgumentException("Unexpected case: " + requestCase);
    }
    final SetIapSubscriptionResponse response =
        unauthenticatedServiceStub().setIapSubscription(builder.build());
    assertEquals(SetIapSubscriptionResponse.ResponseCase.SUCCESS, response.getResponseCase());
    assertEquals(LEVEL, response.getSuccess().getLevel());
  }

  @Test
  void setIapSubscriptionNoRequest() {
    GrpcTestUtils.assertStatusInvalidArgument(
        () -> unauthenticatedServiceStub().setIapSubscription(
            SetIapSubscriptionRequest.newBuilder()
                .setSubscriberId(SUBSCRIBER_ID)
                .build()));
  }

  static Stream<Arguments> setIapSubscriptionExceptions() {
    return Stream.of(
        Arguments.of(new PurchaseNotFoundException(),
            SetIapSubscriptionResponse.ResponseCase.SUBSCRIBER_NOT_FOUND),
        Arguments.of(new PurchaseProcessorConflictException(),
            SetIapSubscriptionResponse.ResponseCase.SUBSCRIPTION_PROCESSOR_CONFLICT),
        Arguments.of(new PurchasePaymentRequiredException(PaymentProvider.GOOGLE_PLAY_BILLING),
            SetIapSubscriptionResponse.ResponseCase.PAYMENT_REQUIRED),
        Arguments.of(new PurchaseInvalidArgumentsException("invalid"),
            SetIapSubscriptionResponse.ResponseCase.INVALID_TRANSACTION)
    );
  }

  @ParameterizedTest
  @MethodSource
  void setIapSubscriptionExceptions(final PurchaseException exception,
      final SetIapSubscriptionResponse.ResponseCase expectedCase)
      throws PurchaseException, RateLimitExceededException {
    doThrow(exception).when(subscriptionManager).updateAppStoreTransactionId(any(), any(), any());
    final SetIapSubscriptionResponse response = unauthenticatedServiceStub().setIapSubscription(
        SetIapSubscriptionRequest.newBuilder()
            .setSubscriberId(SUBSCRIBER_ID)
            .setAppStore(SetIapSubscriptionRequest.AppStorePurchase.newBuilder()
                .setOriginalTransactionId("test-transaction-id").build())
            .build());
    assertEquals(expectedCase, response.getResponseCase());
  }

  @Test
  void getBankMandate() {
    when(bankMandateTranslator.translate(any(), eq(BankTransferType.SEPA_DEBIT))).thenReturn("test-mandate");
    final GetBankMandateResponse response = unauthenticatedServiceStub().getBankMandate(
        GetBankMandateRequest.newBuilder()
            .setBankTransferType(org.signal.chat.purchase.BankTransferType.BANK_TRANSFER_TYPE_SEPA_DEBIT)
            .build());
    assertEquals("test-mandate", response.getMandate());
  }

  private DonationPermit getDonationPermit() {
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(1);
    final DonationPermitRequest permitRequest = context.request();

    final DonationPermitResponse permitResponse = donationPermitsManager.issue(permitRequest);

    try {
      final List<DonationPermit> donationPermits = context.receive(permitResponse,
          DONATION_PERMITS_SECRET_PARAMS.getPublicParams(), clock.instant());

      return donationPermits.getFirst();
    } catch (final VerificationFailedException e) {
      throw new AssertionError("The permit was correctly requested and issued in this method", e);
    }
  }

}
