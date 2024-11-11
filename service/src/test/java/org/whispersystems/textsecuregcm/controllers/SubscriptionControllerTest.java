/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stripe.exception.ApiException;
import com.stripe.model.PaymentIntent;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController.GetBankMandateResponse;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController.GetSubscriptionConfigurationResponse;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.SubscriptionExceptionMapper;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager.PayPalOneTimePaymentApprovalDetails;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.PaymentStatus;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ExtendWith(DropwizardExtensionsSupport.class)
class SubscriptionControllerTest {

  private static final Clock CLOCK = mock(Clock.class);

  private static final ObjectMapper YAML_MAPPER = SystemMapper.yamlMapper();

  private static final SubscriptionConfiguration SUBSCRIPTION_CONFIG = ConfigHelper.getSubscriptionConfig();
  private static final OneTimeDonationConfiguration ONETIME_CONFIG = ConfigHelper.getOneTimeConfig();
  private static final Subscriptions SUBSCRIPTIONS = mock(Subscriptions.class);
  private static final StripeManager STRIPE_MANAGER = MockUtils.buildMock(StripeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.STRIPE));
  private static final BraintreeManager BRAINTREE_MANAGER = MockUtils.buildMock(BraintreeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.BRAINTREE));
  private static final GooglePlayBillingManager PLAY_MANAGER = MockUtils.buildMock(GooglePlayBillingManager.class,
      mgr -> when(mgr.getProvider()).thenReturn(PaymentProvider.GOOGLE_PLAY_BILLING));
  private static final AppleAppStoreManager APPSTORE_MANAGER = MockUtils.buildMock(AppleAppStoreManager.class,
      mgr -> when(mgr.getProvider()).thenReturn(PaymentProvider.APPLE_APP_STORE));
  private static final PaymentIntent PAYMENT_INTENT = mock(PaymentIntent.class);
  private static final ServerZkReceiptOperations ZK_OPS = mock(ServerZkReceiptOperations.class);
  private static final IssuedReceiptsManager ISSUED_RECEIPTS_MANAGER = mock(IssuedReceiptsManager.class);
  private static final OneTimeDonationsManager ONE_TIME_DONATIONS_MANAGER = mock(OneTimeDonationsManager.class);
  private static final BadgeTranslator BADGE_TRANSLATOR = mock(BadgeTranslator.class);
  private static final BankMandateTranslator BANK_MANDATE_TRANSLATOR = mock(BankMandateTranslator.class);
  private final static SubscriptionController SUBSCRIPTION_CONTROLLER = new SubscriptionController(CLOCK,
      SUBSCRIPTION_CONFIG, ONETIME_CONFIG,
      new SubscriptionManager(SUBSCRIPTIONS, List.of(STRIPE_MANAGER, BRAINTREE_MANAGER, PLAY_MANAGER, APPSTORE_MANAGER),
          ZK_OPS, ISSUED_RECEIPTS_MANAGER), STRIPE_MANAGER, BRAINTREE_MANAGER, PLAY_MANAGER, APPSTORE_MANAGER,
      BADGE_TRANSLATOR, BANK_MANDATE_TRANSLATOR);
  private static final OneTimeDonationController ONE_TIME_CONTROLLER = new OneTimeDonationController(CLOCK,
      ONETIME_CONFIG, STRIPE_MANAGER, BRAINTREE_MANAGER, ZK_OPS, ISSUED_RECEIPTS_MANAGER, ONE_TIME_DONATIONS_MANAGER);
  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(SubscriptionExceptionMapper.class)
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(SUBSCRIPTION_CONTROLLER)
      .addResource(ONE_TIME_CONTROLLER)
      .build();

  @BeforeEach
  void setUp() {
    reset(CLOCK, SUBSCRIPTIONS, STRIPE_MANAGER, BRAINTREE_MANAGER, ZK_OPS, ISSUED_RECEIPTS_MANAGER, BADGE_TRANSLATOR);

    when(STRIPE_MANAGER.getProvider()).thenReturn(PaymentProvider.STRIPE);
    when(BRAINTREE_MANAGER.getProvider()).thenReturn(PaymentProvider.BRAINTREE);

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
    final PayPalOneTimePaymentApprovalDetails payPalOneTimePaymentApprovalDetails = mock(PayPalOneTimePaymentApprovalDetails.class);
    when(BRAINTREE_MANAGER.getSupportedCurrenciesForPaymentMethod(PaymentMethod.PAYPAL))
        .thenReturn(Set.of("usd", "jpy", "bif", "eur"));
    when(BRAINTREE_MANAGER.createOneTimePayment(anyString(), anyLong(), anyString(), anyString(), anyString()))
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
        .thenReturn(CompletableFuture.failedFuture(new SubscriptionException.ProcessorException(PaymentProvider.BRAINTREE,
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

  @Nested
  class SetSubscriptionLevel {

    private final long levelId = 5L;
    private final String currency = "jpy";

    private String subscriberId;

    @BeforeEach
    void setUp() {
      when(CLOCK.instant()).thenReturn(Instant.now());

      final byte[] subscriberUserAndKey = new byte[32];
      Arrays.fill(subscriberUserAndKey, (byte) 1);
      subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

      final ProcessorCustomer processorCustomer = new ProcessorCustomer("testCustomerId", PaymentProvider.STRIPE);

      final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
          Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
          Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
          Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID, b(processorCustomer.toDynamoBytes())
      );
      final Subscriptions.Record record = Subscriptions.Record.from(
          Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
      when(SUBSCRIPTIONS.get(eq(Arrays.copyOfRange(subscriberUserAndKey, 0, 16)), any()))
          .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

      when(SUBSCRIPTIONS.subscriptionCreated(any(), any(), any(), anyLong()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void createSubscriptionSuccess() {
      when(STRIPE_MANAGER.createSubscription(any(), any(), anyLong(), anyLong()))
          .thenReturn(CompletableFuture.completedFuture(mock(CustomerAwareSubscriptionPaymentProcessor.SubscriptionId.class)));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void createSubscriptionProcessorDeclined() {
      when(STRIPE_MANAGER.createSubscription(any(), any(), anyLong(), anyLong()))
          .thenReturn(CompletableFuture.failedFuture(new SubscriptionException.ProcessorException(PaymentProvider.STRIPE,
              new ChargeFailure("card_declined", "Insufficient funds", null, null, null))));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(SubscriptionExceptionMapper.PROCESSOR_ERROR_STATUS_CODE);

      final Map responseMap = response.readEntity(Map.class);
      assertThat(responseMap.get("processor")).isEqualTo("STRIPE");
      assertThat(responseMap.get("chargeFailure")).asInstanceOf(
              InstanceOfAssertFactories.map(String.class, Object.class))
          .extracting("code")
          .isEqualTo("card_declined");
    }

    @Test
    void missingCustomerId() {
      final byte[] subscriberUserAndKey = new byte[32];
      Arrays.fill(subscriberUserAndKey, (byte) 1);
      subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

      final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
          Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
          Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
          // missing processor:customer field
      );
      final Subscriptions.Record record = Subscriptions.Record.from(
          Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
      when(SUBSCRIPTIONS.get(eq(Arrays.copyOfRange(subscriberUserAndKey, 0, 16)), any()))
          .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));
      assertThat(response.getStatus()).isEqualTo(409);
      assertThat(response.readEntity(Map.class)).containsOnlyKeys("code", "message");
    }

    @Test
    void wrongProcessor() {
      final byte[] subscriberUserAndKey = new byte[32];
      Arrays.fill(subscriberUserAndKey, (byte) 1);
      subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

      final ProcessorCustomer processorCustomer = new ProcessorCustomer("testCustomerId", PaymentProvider.BRAINTREE);
      final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
          Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
          Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
          Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID, b(processorCustomer.toDynamoBytes())
      );
      final Subscriptions.Record record = Subscriptions.Record.from(
          Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
      when(SUBSCRIPTIONS.get(eq(Arrays.copyOfRange(subscriberUserAndKey, 0, 16)), any()))
          .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

      final Response response = RESOURCE_EXTENSION
          .target(String.format("/v1/subscription/%s/create_payment_method", subscriberId))
          .request()
          .post(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(409);
      assertThat(response.readEntity(Map.class)).containsOnlyKeys("code", "message");
    }

    @Test
    void stripePaymentIntentRequiresAction() {
      final ApiException stripeException = new ApiException("Payment intent requires action",
          UUID.randomUUID().toString(), "subscription_payment_intent_requires_action", 400, new Exception());
      when(STRIPE_MANAGER.createSubscription(any(), any(), anyLong(), anyLong()))
          .thenReturn(CompletableFuture.failedFuture(new CompletionException(stripeException)));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(400);

      assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelErrorResponse.class))
          .satisfies(errorResponse ->
              assertThat(errorResponse.errors())
                  .anySatisfy(error ->
                      assertThat(error.type())
                          .isEqualTo(
                              SubscriptionController.SetSubscriptionLevelErrorResponse.Error.Type.PAYMENT_REQUIRES_ACTION)));
    }
  }

  @Test
  void createSubscriber() {
    when(CLOCK.instant()).thenReturn(Instant.now());

    // basic create
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    when(SUBSCRIPTIONS.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        Subscriptions.GetResult.NOT_STORED));

    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(record));

    final Response createResponse = RESOURCE_EXTENSION.target(String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));
    assertThat(createResponse.getStatus()).isEqualTo(200);

    // creating should be idempotent
    when(SUBSCRIPTIONS.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        Subscriptions.GetResult.found(record)));
    when(SUBSCRIPTIONS.accessedAt(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response idempotentCreateResponse = RESOURCE_EXTENSION.target(
            String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));
    assertThat(idempotentCreateResponse.getStatus()).isEqualTo(200);

    // when the manager returns `null`, it means there was a password mismatch from the storage layer `create`.
    // this could happen if there is a race between two concurrent `create` requests for the same user ID
    when(SUBSCRIPTIONS.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        Subscriptions.GetResult.NOT_STORED));
    when(SUBSCRIPTIONS.create(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response managerCreateNullResponse = RESOURCE_EXTENSION.target(
            String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));
    assertThat(managerCreateNullResponse.getStatus()).isEqualTo(403);

    final byte[] subscriberUserAndMismatchedKey = new byte[32];
    Arrays.fill(subscriberUserAndMismatchedKey, 0, 16, (byte) 1);
    Arrays.fill(subscriberUserAndMismatchedKey, 16, 32, (byte) 2);
    final String mismatchedSubscriberId = Base64.getEncoder().encodeToString(subscriberUserAndMismatchedKey);

    // a password mismatch for an existing record
    when(SUBSCRIPTIONS.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        Subscriptions.GetResult.PASSWORD_MISMATCH));

    final Response passwordMismatchResponse = RESOURCE_EXTENSION.target(
            String.format("/v1/subscription/%s", mismatchedSubscriberId))
        .request()
        .put(Entity.json(""));

    assertThat(passwordMismatchResponse.getStatus()).isEqualTo(403);

    // invalid request data is a 404
    final byte[] malformedUserAndKey = new byte[16];
    Arrays.fill(malformedUserAndKey, (byte) 1);
    final String malformedUserId = Base64.getEncoder().encodeToString(malformedUserAndKey);

    final Response malformedUserAndKeyResponse = RESOURCE_EXTENSION.target(
            String.format("/v1/subscription/%s", malformedUserId))
        .request()
        .put(Entity.json(""));

    assertThat(malformedUserAndKeyResponse.getStatus()).isEqualTo(404);
  }

  @Test
  void createPaymentMethod() {
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        Subscriptions.GetResult.NOT_STORED));

    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    final Response createSubscriberResponse = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));

    assertThat(createSubscriberResponse.getStatus()).isEqualTo(200);

    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final String customerId = "some-customer-id";
    final ProcessorCustomer customer = new ProcessorCustomer(
        customerId, PaymentProvider.STRIPE);
    when(STRIPE_MANAGER.createCustomer(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(customer));

    final Map<String, AttributeValue> dynamoItemWithProcessorCustomer = new HashMap<>(dynamoItem);
    dynamoItemWithProcessorCustomer.put(Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, PaymentProvider.STRIPE).toDynamoBytes()));
    final Subscriptions.Record recordWithCustomerId = Subscriptions.Record.from(record.user,
        dynamoItemWithProcessorCustomer);

    when(SUBSCRIPTIONS.setProcessorAndCustomerId(any(Subscriptions.Record.class), any(),
        any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(recordWithCustomerId));

    final String clientSecret = "some-client-secret";
    when(STRIPE_MANAGER.createPaymentMethodSetupToken(customerId))
        .thenReturn(CompletableFuture.completedFuture(clientSecret));

    final SubscriptionController.CreatePaymentMethodResponse createPaymentMethodResponse = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/create_payment_method", subscriberId))
        .request()
        .post(Entity.json(""))
        .readEntity(SubscriptionController.CreatePaymentMethodResponse.class);

    assertThat(createPaymentMethodResponse.processor()).isEqualTo(PaymentProvider.STRIPE);
    assertThat(createPaymentMethodResponse.clientSecret()).isEqualTo(clientSecret);

  }

  @Test
  void setSubscriptionLevelMissingProcessorCustomer() {
    // set up record
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    // set up mocks
    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/level/%d/%s/%s", subscriberId, 5, "usd", "abcd"))
        .request()
        .put(Entity.json(""));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @ParameterizedTest
  @CsvSource({
      "5, M1",
      "15, M2",
      "35, M3",
      "201, M4",
  })
  void setSubscriptionLevel(long levelId, String expectedProcessorId) {
    // set up record
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final String customerId = "customer";
    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, PaymentProvider.BRAINTREE).toDynamoBytes())
    );
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    // set up mocks
    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    when(BRAINTREE_MANAGER.createSubscription(any(), any(), anyLong(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(new CustomerAwareSubscriptionPaymentProcessor.SubscriptionId(
            "subscription")));
    when(SUBSCRIPTIONS.subscriptionCreated(any(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/level/%d/%s/%s", subscriberId, levelId, "usd", "abcd"))
        .request()
        .put(Entity.json(""));

    verify(BRAINTREE_MANAGER).createSubscription(eq(customerId), eq(expectedProcessorId), eq(levelId), eq(0L));
    verifyNoMoreInteractions(BRAINTREE_MANAGER);

    assertThat(response.getStatus()).isEqualTo(200);

    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelSuccessResponse.class))
        .extracting(SubscriptionController.SetSubscriptionLevelSuccessResponse::level)
        .isEqualTo(levelId);
  }

  @ParameterizedTest
  @MethodSource
  void setSubscriptionLevelExistingSubscription(final String existingCurrency, final long existingLevel,
      final String requestCurrency, final long requestLevel, final boolean expectUpdate) {

    // set up record
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final String customerId = "customer";
    final String existingSubscriptionId = "existingSubscription";
    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, PaymentProvider.BRAINTREE).toDynamoBytes()),
        Subscriptions.KEY_SUBSCRIPTION_ID, s(existingSubscriptionId)
    );
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    // set up mocks
    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final Object subscriptionObj = new Object();
    when(BRAINTREE_MANAGER.getSubscription(any()))
        .thenReturn(CompletableFuture.completedFuture(subscriptionObj));
    when(BRAINTREE_MANAGER.getLevelAndCurrencyForSubscription(subscriptionObj))
        .thenReturn(CompletableFuture.completedFuture(
            new CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency(existingLevel, existingCurrency)));
    final String updatedSubscriptionId = "updatedSubscriptionId";

    if (expectUpdate) {
      when(BRAINTREE_MANAGER.updateSubscription(any(), any(), anyLong(), anyString()))
          .thenReturn(CompletableFuture.completedFuture(new CustomerAwareSubscriptionPaymentProcessor.SubscriptionId(
              updatedSubscriptionId)));
      when(SUBSCRIPTIONS.subscriptionLevelChanged(any(), any(), anyLong(), anyString()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    final String idempotencyKey = "abcd";
    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/level/%d/%s/%s", subscriberId, requestLevel, requestCurrency,
            idempotencyKey))
        .request()
        .put(Entity.json(""));

    verify(BRAINTREE_MANAGER).getSubscription(any());
    verify(BRAINTREE_MANAGER).getLevelAndCurrencyForSubscription(any());

    if (expectUpdate) {
      verify(BRAINTREE_MANAGER).updateSubscription(any(), any(), eq(requestLevel), eq(idempotencyKey));
      verify(SUBSCRIPTIONS).subscriptionLevelChanged(any(), any(), eq(requestLevel), eq(updatedSubscriptionId));
    }

    verifyNoMoreInteractions(BRAINTREE_MANAGER);

    assertThat(response.getStatus()).isEqualTo(200);

    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelSuccessResponse.class))
        .extracting(SubscriptionController.SetSubscriptionLevelSuccessResponse::level)
        .isEqualTo(requestLevel);
  }

  static Stream<Arguments> setSubscriptionLevelExistingSubscription() {
    return Stream.of(
        Arguments.of("usd", 5, "usd", 5, false),
        Arguments.of("usd", 5, "jpy", 5, true),
        Arguments.of("usd", 5, "usd", 15, true),
        Arguments.of("usd", 5, "jpy", 15, true),
        Arguments.of("usd", 201, "usd", 201, false),
        Arguments.of("usd", 201, "jpy", 201, true)
    );
  }

  @Test
  public void changeSubscriptionLevelInvalid() {
    // set up record
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final String customerId = "customer";
    final String existingSubscriptionId = "existingSubscription";
    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, PaymentProvider.BRAINTREE).toDynamoBytes()),
        Subscriptions.KEY_SUBSCRIPTION_ID, s(existingSubscriptionId));
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTIONS.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final Object subscriptionObj = new Object();
    when(BRAINTREE_MANAGER.getSubscription(any()))
        .thenReturn(CompletableFuture.completedFuture(subscriptionObj));
    when(BRAINTREE_MANAGER.getLevelAndCurrencyForSubscription(subscriptionObj))
        .thenReturn(CompletableFuture.completedFuture(
            new CustomerAwareSubscriptionPaymentProcessor.LevelAndCurrency(201, "usd")));

    // Try to change from a backup subscription (201) to a donation subscription (5)
    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/level/%d/%s/%s", subscriberId, 5, "usd", "abcd"))
        .request()
        .put(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelErrorResponse.class))
        .extracting(resp -> resp.errors())
        .asInstanceOf(InstanceOfAssertFactories.list(SubscriptionController.SetSubscriptionLevelErrorResponse.Error.class))
        .hasSize(1).first()
        .extracting(error -> error.type())
        .isEqualTo(SubscriptionController.SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_LEVEL);
  }

  @Test
  public void setAppStoreTransactionId() {
    final String originalTxId = "aTxId";
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final byte[] user = Arrays.copyOfRange(subscriberUserAndKey, 0, 16);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final Instant now = Instant.now();
    when(CLOCK.instant()).thenReturn(now);

    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()));

    final Subscriptions.Record record = Subscriptions.Record.from(user, dynamoItem);

    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    when(APPSTORE_MANAGER.validateTransaction(eq(originalTxId)))
        .thenReturn(CompletableFuture.completedFuture(99L));

    when(SUBSCRIPTIONS.setIapPurchase(any(), any(), anyString(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/appstore/%s", subscriberId, originalTxId))
        .request()
        .post(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelSuccessResponse.class).level())
        .isEqualTo(99L);

    verify(SUBSCRIPTIONS, times(1)).setIapPurchase(
        any(),
        eq(new ProcessorCustomer(originalTxId, PaymentProvider.APPLE_APP_STORE)),
        eq(originalTxId),
        eq(99L),
        eq(now));
  }


  @Test
  public void setPlayPurchaseToken() {
    final String purchaseToken = "aPurchaseToken";
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final byte[] user = Arrays.copyOfRange(subscriberUserAndKey, 0, 16);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final Instant now = Instant.now();
    when(CLOCK.instant()).thenReturn(now);

    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final Subscriptions.Record record = Subscriptions.Record.from(user, dynamoItem);
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final GooglePlayBillingManager.ValidatedToken validatedToken = mock(GooglePlayBillingManager.ValidatedToken.class);
    when(validatedToken.getLevel()).thenReturn(99L);
    when(validatedToken.acknowledgePurchase()).thenReturn(CompletableFuture.completedFuture(null));
    when(PLAY_MANAGER.validateToken(eq(purchaseToken))).thenReturn(CompletableFuture.completedFuture(validatedToken));

    when(SUBSCRIPTIONS.setIapPurchase(any(), any(), anyString(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/playbilling/%s", subscriberId, purchaseToken))
        .request()
        .post(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelSuccessResponse.class).level())
        .isEqualTo(99L);

    verify(SUBSCRIPTIONS, times(1)).setIapPurchase(
        any(),
        eq(new ProcessorCustomer(purchaseToken, PaymentProvider.GOOGLE_PLAY_BILLING)),
        eq(purchaseToken),
        eq(99L),
        eq(now));
  }

  @Test
  public void replacePlayPurchaseToken() {
    final String oldPurchaseToken = "oldPurchaseToken";
    final String newPurchaseToken = "newPurchaseToken";
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final byte[] user = Arrays.copyOfRange(subscriberUserAndKey, 0, 16);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final Instant now = Instant.now();
    when(CLOCK.instant()).thenReturn(now);

    final ProcessorCustomer oldPc = new ProcessorCustomer(oldPurchaseToken, PaymentProvider.GOOGLE_PLAY_BILLING);
    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID, b(oldPc.toDynamoBytes()));
    final Subscriptions.Record record = Subscriptions.Record.from(user, dynamoItem);
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));

    final GooglePlayBillingManager.ValidatedToken validatedToken = mock(GooglePlayBillingManager.ValidatedToken.class);
    when(validatedToken.getLevel()).thenReturn(99L);
    when(validatedToken.acknowledgePurchase()).thenReturn(CompletableFuture.completedFuture(null));

    when(PLAY_MANAGER.validateToken(eq(newPurchaseToken))).thenReturn(CompletableFuture.completedFuture(validatedToken));
    when(PLAY_MANAGER.cancelAllActiveSubscriptions(eq(oldPurchaseToken)))
        .thenReturn(CompletableFuture.completedFuture(null));

    when(SUBSCRIPTIONS.setIapPurchase(any(), any(), anyString(), anyLong(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/playbilling/%s", subscriberId, newPurchaseToken))
        .request()
        .post(Entity.json(""));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(SubscriptionController.SetSubscriptionLevelSuccessResponse.class).level())
        .isEqualTo(99L);

    verify(SUBSCRIPTIONS, times(1)).setIapPurchase(
        any(),
        eq(new ProcessorCustomer(newPurchaseToken, PaymentProvider.GOOGLE_PLAY_BILLING)),
        eq(newPurchaseToken),
        eq(99L),
        eq(now));

    verify(PLAY_MANAGER, times(1)).cancelAllActiveSubscriptions(oldPurchaseToken);
  }

  @ParameterizedTest
  @CsvSource({"5, P45D", "201, P13D"})
  public void createReceiptCredential(long level, Duration expectedExpirationWindow)
      throws InvalidInputException, VerificationFailedException {
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    final String customerId = "customer";
    final String subscriptionId = "subscriptionId";
    final Map<String, AttributeValue> dynamoItem = Map.of(Subscriptions.KEY_PASSWORD, b(new byte[16]),
        Subscriptions.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
        Subscriptions.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, PaymentProvider.BRAINTREE).toDynamoBytes()),
        Subscriptions.KEY_SUBSCRIPTION_ID, s(subscriptionId));
    final Subscriptions.Record record = Subscriptions.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    final ReceiptCredentialRequest receiptRequest = new ClientZkReceiptOperations(
        ServerSecretParams.generate().getPublicParams()).createReceiptCredentialRequestContext(
        new ReceiptSerial(new byte[ReceiptSerial.SIZE])).getRequest();
    final ReceiptCredentialResponse receiptCredentialResponse = mock(ReceiptCredentialResponse.class);

    when(CLOCK.instant()).thenReturn(Instant.now());
    when(SUBSCRIPTIONS.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Subscriptions.GetResult.found(record)));
    when(BRAINTREE_MANAGER.getReceiptItem(subscriptionId)).thenReturn(
        CompletableFuture.completedFuture(new CustomerAwareSubscriptionPaymentProcessor.ReceiptItem(
            "itemId",
            PaymentTime.periodStart(Instant.ofEpochSecond(10).plus(Duration.ofDays(1))),
            level
        )));
    when(ISSUED_RECEIPTS_MANAGER.recordIssuance(eq("itemId"), eq(PaymentProvider.BRAINTREE), eq(receiptRequest), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(ZK_OPS.issueReceiptCredential(any(), anyLong(), eq(level))).thenReturn(receiptCredentialResponse);
    when(receiptCredentialResponse.serialize()).thenReturn(new byte[0]);
    final Response response = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s/receipt_credentials", subscriberId))
        .request()
        .post(Entity.json(new SubscriptionController.GetReceiptCredentialsRequest(receiptRequest.serialize())));
    assertThat(response.getStatus()).isEqualTo(200);

    long expectedExpiration = Instant.EPOCH
        // Truncated current time is day 1
        .plus(Duration.ofDays(1))
        // Expected expiration window
        .plus(expectedExpirationWindow)
        // + one day to forgive skew
        .plus(Duration.ofDays(1)).getEpochSecond();
    verify(ZK_OPS).issueReceiptCredential(any(), eq(expectedExpiration), eq(level));
  }

  @Test
  void testGetBankMandate() {
    when(BANK_MANDATE_TRANSLATOR.translate(any(), any())).thenReturn("bankMandate");
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/bank_mandate/sepa_debit")
        .request()
        .get();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(GetBankMandateResponse.class).mandate()).isEqualTo("bankMandate");
  }

  @Test
  void testGetBankMandateInvalidBankTransferType() {
    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/bank_mandate/ach")
        .request()
        .get();
    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void getSubscriptionConfiguration() {
    when(BADGE_TRANSLATOR.translate(any(), eq("B1"))).thenReturn(new Badge("B1", "cat1", "name1", "desc1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("B2"))).thenReturn(new Badge("B2", "cat2", "name2", "desc2",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("B3"))).thenReturn(new Badge("B3", "cat3", "name3", "desc3",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("BOOST"))).thenReturn(new Badge("BOOST", "boost1", "boost1", "boost1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("GIFT"))).thenReturn(new Badge("GIFT", "gift1", "gift1", "gift1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));

    GetSubscriptionConfigurationResponse response = RESOURCE_EXTENSION.target("/v1/subscription/configuration")
        .request()
        .get(GetSubscriptionConfigurationResponse.class);

    assertThat(response.sepaMaximumEuros()).isEqualTo("10000");
    assertThat(response.currencies()).containsKeys("usd", "jpy", "bif", "eur").satisfies(currencyMap -> {
      assertThat(currencyMap).extractingByKey("usd").satisfies(currency -> {
        assertThat(currency.minimum()).isEqualByComparingTo(
            BigDecimal.valueOf(2.5).setScale(2, RoundingMode.HALF_EVEN));
        assertThat(currency.oneTime()).isEqualTo(
            Map.of("1",
                List.of(BigDecimal.valueOf(5.5).setScale(2, RoundingMode.HALF_EVEN), BigDecimal.valueOf(6),
                    BigDecimal.valueOf(7), BigDecimal.valueOf(8),
                    BigDecimal.valueOf(9), BigDecimal.valueOf(10)), "100",
                List.of(BigDecimal.valueOf(20))));
        assertThat(currency.subscription()).isEqualTo(
            Map.of("5", BigDecimal.valueOf(5), "15", BigDecimal.valueOf(15), "35", BigDecimal.valueOf(35)));
        assertThat(currency.backupSubscription()).isEqualTo(Map.of("201", BigDecimal.valueOf(5)));
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD", "PAYPAL"));
      });

      assertThat(currencyMap).extractingByKey("jpy").satisfies(currency -> {
        assertThat(currency.minimum()).isEqualByComparingTo(
            BigDecimal.valueOf(250));
        assertThat(currency.oneTime()).isEqualTo(
            Map.of("1",
                List.of(BigDecimal.valueOf(550), BigDecimal.valueOf(600),
                    BigDecimal.valueOf(700), BigDecimal.valueOf(800),
                    BigDecimal.valueOf(900), BigDecimal.valueOf(1000)), "100",
                List.of(BigDecimal.valueOf(2000))));
        assertThat(currency.subscription()).isEqualTo(
            Map.of("5", BigDecimal.valueOf(500), "15", BigDecimal.valueOf(1500), "35", BigDecimal.valueOf(3500)));
        assertThat(currency.backupSubscription()).isEqualTo(Map.of("201", BigDecimal.valueOf(500)));
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD", "PAYPAL"));
      });

      assertThat(currencyMap).extractingByKey("bif").satisfies(currency -> {
        assertThat(currency.minimum()).isEqualByComparingTo(
            BigDecimal.valueOf(2500));
        assertThat(currency.oneTime()).isEqualTo(
            Map.of("1",
                List.of(BigDecimal.valueOf(5500), BigDecimal.valueOf(6000),
                    BigDecimal.valueOf(7000), BigDecimal.valueOf(8000),
                    BigDecimal.valueOf(9000), BigDecimal.valueOf(10000)), "100",
                List.of(BigDecimal.valueOf(20000))));
        assertThat(currency.subscription()).isEqualTo(
            Map.of("5", BigDecimal.valueOf(5000), "15", BigDecimal.valueOf(15000), "35", BigDecimal.valueOf(35000)));
        assertThat(currency.backupSubscription()).isEqualTo(Map.of("201", BigDecimal.valueOf(5000)));
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD"));
      });

      assertThat(currencyMap).extractingByKey("eur").satisfies(currency -> {
        assertThat(currency.minimum()).isEqualByComparingTo(
            BigDecimal.valueOf(3));
        assertThat(currency.oneTime()).isEqualTo(
            Map.of("1",
                List.of(BigDecimal.valueOf(5), BigDecimal.valueOf(10),
                    BigDecimal.valueOf(20), BigDecimal.valueOf(30), BigDecimal.valueOf(50), BigDecimal.valueOf(100)), "100",
                List.of(BigDecimal.valueOf(5))));
        assertThat(currency.subscription()).isEqualTo(
            Map.of("5", BigDecimal.valueOf(5), "15", BigDecimal.valueOf(15), "35", BigDecimal.valueOf(35)));
        assertThat(currency.backupSubscription()).isEqualTo(Map.of("201", BigDecimal.valueOf(5)));
        final List<String> expectedPaymentMethods = List.of("CARD", "SEPA_DEBIT", "IDEAL");
        assertThat(currency.supportedPaymentMethods()).isEqualTo(expectedPaymentMethods);
      });
    });

    assertThat(response.levels()).containsKeys("1", "5", "15", "35", "100").satisfies(levelsMap -> {
      assertThat(levelsMap).extractingByKey("1").satisfies(
          level -> assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge)
              .satisfies(badge -> {
                assertThat(badge.getId()).isEqualTo("BOOST");
                assertThat(badge.getName()).isEqualTo("boost1");
              }));

      assertThat(levelsMap).extractingByKey("100").satisfies(
          level -> assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge)
              .satisfies(badge -> {
                assertThat(badge.getId()).isEqualTo("GIFT");
                assertThat(badge.getName()).isEqualTo("gift1");
              }));

      assertThat(levelsMap).extractingByKey("5").satisfies(level -> {
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge)
            .satisfies(badge -> {
              assertThat(badge.getId()).isEqualTo("B1");
              assertThat(badge.getName()).isEqualTo("name1");
            });
      });

      assertThat(levelsMap).extractingByKey("15").satisfies(level ->
          assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge)
              .satisfies(badge -> {
                assertThat(badge.getId()).isEqualTo("B2");
                assertThat(badge.getName()).isEqualTo("name2");
              }));

      assertThat(levelsMap).extractingByKey("35").satisfies(level -> {
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge)
            .satisfies(badge -> {
              assertThat(badge.getId()).isEqualTo("B3");
              assertThat(badge.getName()).isEqualTo("name3");
            });
      });
    });

    assertThat(response.backup().levels()).containsOnlyKeys("201").extractingByKey("201").satisfies(configuration -> {
      assertThat(configuration.storageAllowanceBytes()).isEqualTo(BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES);
      assertThat(configuration.playProductId()).isEqualTo("testPlayProductId");
      assertThat(configuration.mediaTtlDays()).isEqualTo(40);
    });
    assertThat(response.backup().freeTierMediaDays()).isEqualTo(30);

    // check the badge vs purchasable badge fields
    // subscription levels are Badge, while one-time levels are PurchasableBadge, which adds `duration`
    Map<String, Object> genericResponse = RESOURCE_EXTENSION.target("/v1/subscription/configuration")
        .request()
        .get(Map.class);

    assertThat(genericResponse.get("levels")).satisfies(levels -> {
      final Set<String> oneTimeLevels = Set.of("1", "100");
      oneTimeLevels.forEach(
          oneTimeLevel -> assertThat((Map<String, Map<String, Map<String, Object>>>) levels)
              .extractingByKey(oneTimeLevel)
              .satisfies(level -> assertThat(level.get("badge")).containsKeys("duration")));

      ((Map<String, ?>) levels).keySet().stream()
          .filter(Predicate.not(oneTimeLevels::contains))
          .forEach(subscriptionLevel ->
              assertThat((Map<String, Map<String, Map<String, Object>>>) levels)
                  .extractingByKey(subscriptionLevel)
                  .satisfies(level -> assertThat(level.get("badge")).doesNotContainKeys("duration")));
    });
  }

  /**
   * Encapsulates {@code static} configuration, to keep the class header simpler and avoid illegal forward references
   */
  private record ConfigHelper() {

    private static SubscriptionConfiguration getSubscriptionConfig() {
      return readValue(SUBSCRIPTION_CONFIG_YAML, SubscriptionConfiguration.class);
    }

    private static OneTimeDonationConfiguration getOneTimeConfig() {
      return readValue(ONETIME_CONFIG_YAML, OneTimeDonationConfiguration.class);
    }

    private static <T> T readValue(String yaml, Class<T> type) {
      try {
        return YAML_MAPPER.readValue(yaml, type);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static final String SUBSCRIPTION_CONFIG_YAML = """
        badgeExpiration: P30D
        badgeGracePeriod: P15D
        backupExpiration: P3D
        backupGracePeriod: P10D
        backupFreeTierMediaDuration: P30D
        backupLevels:
          201:
            playProductId: testPlayProductId
            mediaTtl: P40D
            prices:
              usd:
                amount: '5'
                processorIds:
                  STRIPE: R4
                  BRAINTREE: M4
              jpy:
                amount: '500'
                processorIds:
                  STRIPE: Q4
                  BRAINTREE: N4
              bif:
                amount: '5000'
                processorIds:
                  STRIPE: S4
                  BRAINTREE: O4
              eur:
                amount: '5'
                processorIds:
                  STRIPE: A4
                  BRAINTREE: B4
        levels:
          5:
            badge: B1
            prices:
              usd:
                amount: '5'
                processorIds:
                  STRIPE: R1
                  BRAINTREE: M1
              jpy:
                amount: '500'
                processorIds:
                  STRIPE: Q1
                  BRAINTREE: N1
              bif:
                amount: '5000'
                processorIds:
                  STRIPE: S1
                  BRAINTREE: O1
              eur:
                amount: '5'
                processorIds:
                  STRIPE: A1
                  BRAINTREE: B1
          15:
            badge: B2
            prices:
              usd:
                amount: '15'
                processorIds:
                  STRIPE: R2
                  BRAINTREE: M2
              jpy:
                amount: '1500'
                processorIds:
                  STRIPE: Q2
                  BRAINTREE: N2
              bif:
                amount: '15000'
                processorIds:
                  STRIPE: S2
                  BRAINTREE: O2
              eur:
                amount: '15'
                processorIds:
                  STRIPE: A2
                  BRAINTREE: B2
          35:
            badge: B3
            prices:
              usd:
                amount: '35'
                processorIds:
                  STRIPE: R3
                  BRAINTREE: M3
              jpy:
                amount: '3500'
                processorIds:
                  STRIPE: Q3
                  BRAINTREE: N3
              bif:
                amount: '35000'
                processorIds:
                  STRIPE: S3
                  BRAINTREE: O3
              eur:
                amount: '35'
                processorIds:
                  STRIPE: A3
                  BRAINTREE: B3
        """;

    private static final String ONETIME_CONFIG_YAML = """
        boost:
          level: 1
          expiration: P45D
          badge: BOOST
        gift:
          level: 100
          expiration: P60D
          badge: GIFT
        currencies:
          usd:
            minimum: '2.50' # fractional to test BigDecimal conversion
            gift: '20'
            boosts:
              - '5.50'
              - '6'
              - '7'
              - '8'
              - '9'
              - '10'
          eur:
            minimum: '3'
            gift: '5'
            boosts:
              - '5'
              - '10'
              - '20'
              - '30'
              - '50'
              - '100'
          jpy:
            minimum: '250'
            gift: '2000'
            boosts:
              - '550'
              - '600'
              - '700'
              - '800'
              - '900'
              - '1000'
          bif:
            minimum: '2500'
            gift: '20000'
            boosts:
              - '5500'
              - '6000'
              - '7000'
              - '8000'
              - '9000'
              - '10000'
        sepaMaximumEuros: '10000'
        """;

  }


}
