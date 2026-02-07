/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.stripe.model.PaymentIntent;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.SubscriptionExceptionMapper;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorException;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class OneTimeDonationControllerTest extends AbstractV1SubscriptionControllerTest {

  private static final PaymentIntent PAYMENT_INTENT = mock(PaymentIntent.class);
  private static final PayPalDonationsTranslator PAYPAL_ONE_TIME_DONATION_LINE_ITEM_TRANSLATOR = mock(
      PayPalDonationsTranslator.class);
  private static final OneTimeDonationsManager ONE_TIME_DONATIONS_MANAGER = mock(OneTimeDonationsManager.class);

  private static final OneTimeDonationController ONE_TIME_CONTROLLER = new OneTimeDonationController(CLOCK,
      ONETIME_CONFIG, STRIPE_MANAGER, BRAINTREE_MANAGER, PAYPAL_ONE_TIME_DONATION_LINE_ITEM_TRANSLATOR,
      ZK_OPS, ISSUED_RECEIPTS_MANAGER, ONE_TIME_DONATIONS_MANAGER);

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(SubscriptionExceptionMapper.class)
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(ONE_TIME_CONTROLLER)
      .build();

  @BeforeEach
  void setUp() {
    reset(CLOCK, STRIPE_MANAGER, BRAINTREE_MANAGER, ZK_OPS, PAYPAL_ONE_TIME_DONATION_LINE_ITEM_TRANSLATOR);

    when(STRIPE_MANAGER.getProvider()).thenReturn(PaymentProvider.STRIPE);
    when(BRAINTREE_MANAGER.getProvider()).thenReturn(PaymentProvider.BRAINTREE);
    when(PAYPAL_ONE_TIME_DONATION_LINE_ITEM_TRANSLATOR.translate(any(), any())).thenReturn("Donation to Signal Technology Foundation");

    List.of(STRIPE_MANAGER, BRAINTREE_MANAGER)
        .forEach(manager -> when(manager.supportsPaymentMethod(any()))
            .thenCallRealMethod());
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.CARD))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.SEPA_DEBIT))
        .thenReturn(Set.of("eur"));
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.IDEAL))
        .thenReturn(Set.of("eur"));
    when(BRAINTREE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd", "jpy"));
  }


  @Test
  void testCreateBoostPaymentIntentAmountBelowCurrencyMinimum() {
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.CARD))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("""
              {
                "currency": "USD",
                "amount": 249,
                "level": null
              }
            """));
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.hasEntity()).isTrue();
    final Map responseMap = response.readEntity(Map.class);
    assertThat(responseMap.get("error")).isEqualTo("amount_below_currency_minimum");
    assertThat(responseMap.get("minimum")).isEqualTo("2.50");
  }

  @Test
  void testCreateBoostPaymentIntentAmountAboveSepaLimit() {
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.SEPA_DEBIT))
        .thenReturn(Set.of("eur"));
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("""
              {
                "currency": "EUR",
                "amount": 1000001,
                "level": null,
                "paymentMethod": "SEPA_DEBIT"
              }
            """));
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.hasEntity()).isTrue();

    final Map responseMap = response.readEntity(Map.class);
    assertThat(responseMap.get("error")).isEqualTo("amount_above_sepa_limit");
    assertThat(responseMap.get("maximum")).isEqualTo("10000");
  }

  @Test
  void testCreateBoostPaymentIntentUnsupportedCurrency() {
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.SEPA_DEBIT))
        .thenReturn(Set.of("eur"));
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("""
              {
                "currency": "USD",
                "amount": 3000,
                "level": null,
                "paymentMethod": "SEPA_DEBIT"
              }
            """));
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.hasEntity()).isTrue();

    final Map responseMap = response.readEntity(Map.class);
    assertThat(responseMap.get("error")).isEqualTo("unsupported_currency");
  }

  @Test
  void testCreateBoostPaymentIntentLevelAmountMismatch() {
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.CARD))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("""
              {
                "currency": "USD",
                "amount": 25,
                "level": 100
              }
            """
        ));
    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testCreateBoostPaymentIntent() {
    when(STRIPE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.CARD))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    when(STRIPE_MANAGER.createPaymentIntent(anyString(), anyLong(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(PAYMENT_INTENT));

    String clientSecret = "some_client_secret";
    when(PAYMENT_INTENT.getClientSecret()).thenReturn(clientSecret);

    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("{\"currency\": \"USD\", \"amount\": 300, \"level\": null}"));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testCreateBoostPayPal() {
    final BraintreeManager.PayPalOneTimePaymentApprovalDetails payPalOneTimePaymentApprovalDetails = mock(
        BraintreeManager.PayPalOneTimePaymentApprovalDetails.class);
    when(BRAINTREE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    when(BRAINTREE_MANAGER.createOneTimePayment(anyString(), anyLong(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(payPalOneTimePaymentApprovalDetails));
    when(payPalOneTimePaymentApprovalDetails.approvalUrl()).thenReturn("approvalUrl");
    when(payPalOneTimePaymentApprovalDetails.paymentId()).thenReturn("someId");

    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/paypal/create")
        .request()
        .post(Entity.json("""
              {
                "currency": "USD",
                "amount": 300,
                "cancelUrl": "cancelUrl",
                "returnUrl": "returnUrl"
              }
            """
        ));
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void createBoostReceiptInvalid() {
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/receipt_credentials")
        .request()
        // invalid, request body should have receiptCredentialRequest
        .post(Entity.json("{\"paymentIntentId\": \"foo\"}"));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @ParameterizedTest
  @MethodSource
  void createBoostReceiptPaymentRequired(final ChargeFailure chargeFailure, boolean expectChargeFailure) {
    when(STRIPE_MANAGER.getPaymentDetails(any())).thenReturn(CompletableFuture.completedFuture(new PaymentDetails(
        "id",
        Collections.emptyMap(),
        PaymentStatus.FAILED,
        Instant.now(),
        chargeFailure)
    ));
    Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/receipt_credentials")
        .request()
        .post(Entity.json("""
            {
              "paymentIntentId": "foo",
              "receiptCredentialRequest": "abcd",
              "processor": "STRIPE"
            }
          """));
    assertThat(response.getStatus()).isEqualTo(402);

    if (expectChargeFailure) {
      assertThat(response.readEntity(OneTimeDonationController.CreateBoostReceiptCredentialsErrorResponse.class).chargeFailure()).isEqualTo(chargeFailure);
    } else {
      assertThat(response.readEntity(String.class)).isEqualTo("{}");
    }
  }

  private static Stream<Arguments> createBoostReceiptPaymentRequired() {
    return Stream.of(
        Arguments.of(new ChargeFailure(
            "generic_decline",
            "some failure message",
            null,
            null,
            null
        ), true),
        Arguments.of(null, false)
    );
  }

  @Test
  void confirmPaypalBoostProcessorError() {

    when(BRAINTREE_MANAGER.captureOneTimePayment(anyString(), anyString(), anyString(), anyString(), anyLong(),
        anyLong(), any()))
        .thenReturn(CompletableFuture.failedFuture(new SubscriptionProcessorException(PaymentProvider.BRAINTREE,
            new ChargeFailure("2046", "Declined", null, null, null))));

    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/paypal/confirm")
        .request()
        .post(Entity.json(Map.of("payerId", "payer123",
            "paymentId", "PAYID-456",
            "paymentToken", "EC-789",
            "currency", "usd",
            "amount", 123)));

    assertThat(response.getStatus()).isEqualTo(SubscriptionExceptionMapper.PROCESSOR_ERROR_STATUS_CODE);

    final Map responseMap = response.readEntity(Map.class);
    assertThat(responseMap.get("processor")).isEqualTo("BRAINTREE");
    assertThat(responseMap.get("chargeFailure")).asInstanceOf(
            InstanceOfAssertFactories.map(String.class, Object.class))
        .extracting("code")
        .isEqualTo("2046");
  }

  @Test
  void createBoostReceiptNoRequest() {
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/receipt_credentials")
        .request()
        .post(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(422);
  }

}
