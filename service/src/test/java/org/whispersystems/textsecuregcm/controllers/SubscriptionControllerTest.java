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
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stripe.exception.ApiException;
import com.stripe.model.PaymentIntent;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.badges.LevelTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController.GetLevelsResponse;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController.GetSubscriptionConfigurationResponse;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ExtendWith(DropwizardExtensionsSupport.class)
class SubscriptionControllerTest {

  private static final Clock CLOCK = mock(Clock.class);

  private static final YAMLMapper YAML_MAPPER = new YAMLMapper();

  static {
    YAML_MAPPER.registerModule(new JavaTimeModule());
  }

  private static final SubscriptionConfiguration SUBSCRIPTION_CONFIG = ConfigHelper.getSubscriptionConfig();
  private static final OneTimeDonationConfiguration ONETIME_CONFIG = ConfigHelper.getOneTimeConfig();
  private static final SubscriptionManager SUBSCRIPTION_MANAGER = mock(SubscriptionManager.class);
  private static final StripeManager STRIPE_MANAGER = mock(StripeManager.class);
  private static final PaymentIntent PAYMENT_INTENT = mock(PaymentIntent.class);

  static {
    // this behavior is required by the SubscriptionController constructor
    when(STRIPE_MANAGER.getSupportedCurrencies())
        .thenReturn(Set.of("usd", "jpy", "bif"));
    when(STRIPE_MANAGER.supportsPaymentMethod(any()))
        .thenCallRealMethod();
  }

  private static final BraintreeManager BRAINTREE_MANAGER = mock(BraintreeManager.class);

  private static final ServerZkReceiptOperations ZK_OPS = mock(ServerZkReceiptOperations.class);
  private static final IssuedReceiptsManager ISSUED_RECEIPTS_MANAGER = mock(IssuedReceiptsManager.class);
  private static final BadgeTranslator BADGE_TRANSLATOR = mock(BadgeTranslator.class);
  private static final LevelTranslator LEVEL_TRANSLATOR = mock(LevelTranslator.class);
  private static final SubscriptionController SUBSCRIPTION_CONTROLLER = new SubscriptionController(
      CLOCK, SUBSCRIPTION_CONFIG, ONETIME_CONFIG, SUBSCRIPTION_MANAGER, STRIPE_MANAGER, BRAINTREE_MANAGER, ZK_OPS,
      ISSUED_RECEIPTS_MANAGER, BADGE_TRANSLATOR, LEVEL_TRANSLATOR);
  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(CompletionExceptionMapper.class)
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(Set.of(
          AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(SUBSCRIPTION_CONTROLLER)
      .build();

  @BeforeEach
  void setUp() {
    when(STRIPE_MANAGER.getProcessor()).thenReturn(SubscriptionProcessor.STRIPE);
  }

  @AfterEach
  void tearDown() {
    reset(CLOCK, SUBSCRIPTION_MANAGER, STRIPE_MANAGER, ZK_OPS, ISSUED_RECEIPTS_MANAGER,
        BADGE_TRANSLATOR, LEVEL_TRANSLATOR);
  }

  @Test
  void testCreateBoostPaymentIntentAmountBelowCurrencyMinimum() {
    when(STRIPE_MANAGER.supportsCurrency("usd")).thenReturn(true);
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
    assertThat(response.readEntity(Map.class))
        .isNotNull()
        .containsAllEntriesOf(Map.of(
            "error", "amount_below_currency_minimum",
            "minimum", "2.50"
        ));
  }

  @Test
  void testCreateBoostPaymentIntentLevelAmountMismatch() {
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
    when(STRIPE_MANAGER.createPaymentIntent(anyString(), anyLong(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(PAYMENT_INTENT));
    when(STRIPE_MANAGER.supportsCurrency("usd")).thenReturn(true);

    String clientSecret = "some_client_secret";
    when(PAYMENT_INTENT.getClientSecret()).thenReturn(clientSecret);

    final Response response = RESOURCE_EXTENSION.target("/v1/subscription/boost/create")
        .request()
        .post(Entity.json("{\"currency\": \"USD\", \"amount\": 300, \"level\": null}"));
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

      final ProcessorCustomer processorCustomer = new ProcessorCustomer("testCustomerId", SubscriptionProcessor.STRIPE);

      final Map<String, AttributeValue> dynamoItem = Map.of(SubscriptionManager.KEY_PASSWORD, b(new byte[16]),
          SubscriptionManager.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
          SubscriptionManager.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond()),
          SubscriptionManager.KEY_PROCESSOR_ID_CUSTOMER_ID, b(processorCustomer.toDynamoBytes())
      );
      final SubscriptionManager.Record record = SubscriptionManager.Record.from(
          Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
      when(SUBSCRIPTION_MANAGER.get(eq(Arrays.copyOfRange(subscriberUserAndKey, 0, 16)), any()))
          .thenReturn(CompletableFuture.completedFuture(SubscriptionManager.GetResult.found(record)));

      when(SUBSCRIPTION_MANAGER.subscriptionCreated(any(), any(), any(), anyLong()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void success() {
      when(STRIPE_MANAGER.createSubscription(any(), any(), anyLong(), anyLong()))
          .thenReturn(CompletableFuture.completedFuture(mock(SubscriptionProcessorManager.SubscriptionId.class)));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void missingCustomerId() {
      final byte[] subscriberUserAndKey = new byte[32];
      Arrays.fill(subscriberUserAndKey, (byte) 1);
      subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

      final Map<String, AttributeValue> dynamoItem = Map.of(SubscriptionManager.KEY_PASSWORD, b(new byte[16]),
          SubscriptionManager.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
          SubscriptionManager.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
          // missing processor:customer field
      );
      final SubscriptionManager.Record record = SubscriptionManager.Record.from(
          Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
      when(SUBSCRIPTION_MANAGER.get(eq(Arrays.copyOfRange(subscriberUserAndKey, 0, 16)), any()))
          .thenReturn(CompletableFuture.completedFuture(SubscriptionManager.GetResult.found(record)));

      final String level = String.valueOf(levelId);
      final String idempotencyKey = UUID.randomUUID().toString();
      final Response response = RESOURCE_EXTENSION.target(
              String.format("/v1/subscription/%s/level/%s/%s/%s", subscriberId, level, currency, idempotencyKey))
          .request()
          .put(Entity.json(""));

      assertThat(response.getStatus()).isEqualTo(409);
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
          .satisfies(errorResponse -> {
            assertThat(errorResponse.getErrors())
                .anySatisfy(error -> {
                  assertThat(error.getType()).isEqualTo(
                      SubscriptionController.SetSubscriptionLevelErrorResponse.Error.Type.PAYMENT_REQUIRES_ACTION);
                });
          });
    }
  }

  @Test
  void createSubscriber() {
    when(CLOCK.instant()).thenReturn(Instant.now());

    // basic create
    final byte[] subscriberUserAndKey = new byte[32];
    Arrays.fill(subscriberUserAndKey, (byte) 1);
    final String subscriberId = Base64.getEncoder().encodeToString(subscriberUserAndKey);

    when(SUBSCRIPTION_MANAGER.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        SubscriptionManager.GetResult.NOT_STORED));

    final Map<String, AttributeValue> dynamoItem = Map.of(SubscriptionManager.KEY_PASSWORD, b(new byte[16]),
        SubscriptionManager.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        SubscriptionManager.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final SubscriptionManager.Record record = SubscriptionManager.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTION_MANAGER.create(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(record));

    final Response createResponse = RESOURCE_EXTENSION.target(String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));
    assertThat(createResponse.getStatus()).isEqualTo(200);

    // creating should be idempotent
    when(SUBSCRIPTION_MANAGER.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        SubscriptionManager.GetResult.found(record)));
    when(SUBSCRIPTION_MANAGER.accessedAt(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response idempotentCreateResponse = RESOURCE_EXTENSION.target(
            String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));
    assertThat(idempotentCreateResponse.getStatus()).isEqualTo(200);

    // when the manager returns `null`, it means there was a password mismatch from the storage layer `create`.
    // this could happen if there is a race between two concurrent `create` requests for the same user ID
    when(SUBSCRIPTION_MANAGER.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        SubscriptionManager.GetResult.NOT_STORED));
    when(SUBSCRIPTION_MANAGER.create(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

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
    when(SUBSCRIPTION_MANAGER.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        SubscriptionManager.GetResult.PASSWORD_MISMATCH));

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
    when(SUBSCRIPTION_MANAGER.get(any(), any())).thenReturn(CompletableFuture.completedFuture(
        SubscriptionManager.GetResult.NOT_STORED));

    final Map<String, AttributeValue> dynamoItem = Map.of(SubscriptionManager.KEY_PASSWORD, b(new byte[16]),
        SubscriptionManager.KEY_CREATED_AT, n(Instant.now().getEpochSecond()),
        SubscriptionManager.KEY_ACCESSED_AT, n(Instant.now().getEpochSecond())
    );
    final SubscriptionManager.Record record = SubscriptionManager.Record.from(
        Arrays.copyOfRange(subscriberUserAndKey, 0, 16), dynamoItem);
    when(SUBSCRIPTION_MANAGER.create(any(), any(), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(record));

    final Response createSubscriberResponse = RESOURCE_EXTENSION
        .target(String.format("/v1/subscription/%s", subscriberId))
        .request()
        .put(Entity.json(""));

    assertThat(createSubscriberResponse.getStatus()).isEqualTo(200);

    when(SUBSCRIPTION_MANAGER.get(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(SubscriptionManager.GetResult.found(record)));

    final String customerId = "some-customer-id";
    final ProcessorCustomer customer = new ProcessorCustomer(
        customerId, SubscriptionProcessor.STRIPE);
    when(STRIPE_MANAGER.createCustomer(any()))
        .thenReturn(CompletableFuture.completedFuture(customer));

    final Map<String, AttributeValue> dynamoItemWithProcessorCustomer = new HashMap<>(dynamoItem);
    dynamoItemWithProcessorCustomer.put(SubscriptionManager.KEY_PROCESSOR_ID_CUSTOMER_ID,
        b(new ProcessorCustomer(customerId, SubscriptionProcessor.STRIPE).toDynamoBytes()));
    final SubscriptionManager.Record recordWithCustomerId = SubscriptionManager.Record.from(record.user,
        dynamoItemWithProcessorCustomer);

    when(SUBSCRIPTION_MANAGER.setProcessorAndCustomerId(any(SubscriptionManager.Record.class), any(),
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

    assertThat(createPaymentMethodResponse.processor()).isEqualTo(SubscriptionProcessor.STRIPE);
    assertThat(createPaymentMethodResponse.clientSecret()).isEqualTo(clientSecret);

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
    when(LEVEL_TRANSLATOR.translate(any(), eq("B1"))).thenReturn("Z1");
    when(LEVEL_TRANSLATOR.translate(any(), eq("B2"))).thenReturn("Z2");
    when(LEVEL_TRANSLATOR.translate(any(), eq("B3"))).thenReturn("Z3");

    GetSubscriptionConfigurationResponse response = RESOURCE_EXTENSION.target("/v1/subscription/configuration")
        .request()
        .get(GetSubscriptionConfigurationResponse.class);

    assertThat(response.currencies()).containsKeys("usd", "jpy", "bif").satisfies(currencyMap -> {
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
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD"));
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
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD"));
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
        assertThat(currency.supportedPaymentMethods()).isEqualTo(List.of("CARD"));
      });
    });

    assertThat(response.levels()).containsKeys("1", "5", "15", "35", "100").satisfies(levelsMap -> {
      assertThat(levelsMap).extractingByKey("1").satisfies(level -> {
        assertThat(level.name()).isEqualTo("boost1"); // level name is the same as badge name
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge).satisfies(badge -> {
          assertThat(badge.getId()).isEqualTo("BOOST");
          assertThat(badge.getName()).isEqualTo("boost1");
        });
      });

      assertThat(levelsMap).extractingByKey("100").satisfies(level -> {
        assertThat(level.name()).isEqualTo("gift1"); // level name is the same as badge name
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge).satisfies(badge -> {
          assertThat(badge.getId()).isEqualTo("GIFT");
          assertThat(badge.getName()).isEqualTo("gift1");
        });
      });

      assertThat(levelsMap).extractingByKey("5").satisfies(level -> {
        assertThat(level.name()).isEqualTo("Z1");
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge).satisfies(badge -> {
          assertThat(badge.getId()).isEqualTo("B1");
          assertThat(badge.getName()).isEqualTo("name1");
        });
      });

      assertThat(levelsMap).extractingByKey("15").satisfies(level -> {
        assertThat(level.name()).isEqualTo("Z2");
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge).satisfies(badge -> {
          assertThat(badge.getId()).isEqualTo("B2");
          assertThat(badge.getName()).isEqualTo("name2");
        });
      });

      assertThat(levelsMap).extractingByKey("35").satisfies(level -> {
        assertThat(level.name()).isEqualTo("Z3");
        assertThat(level).extracting(SubscriptionController.LevelConfiguration::badge).satisfies(badge -> {
          assertThat(badge.getId()).isEqualTo("B3");
          assertThat(badge.getName()).isEqualTo("name3");
        });
      });
    });

    // check the badge vs purchasable badge fields
    // subscription levels are Badge, while one-time levels are PurchasableBadge, which adds `duration`
    Map<String, Object> genericResponse = RESOURCE_EXTENSION.target("/v1/subscription/configuration")
        .request()
        .get(Map.class);

    assertThat(genericResponse.get("levels")).satisfies(levels -> {
      final Set<String> oneTimeLevels = Set.of("1", "100");
      oneTimeLevels.forEach(oneTimeLevel -> {
        assertThat((Map<String, Map<String, Map<String, Object>>>) levels).extractingByKey(oneTimeLevel)
            .satisfies(level -> {
              assertThat(level.get("badge")).containsKeys("duration");
            });
      });

      ((Map<String, ?>) levels).keySet().stream()
          .filter(Predicate.not(oneTimeLevels::contains))
          .forEach(subscriptionLevel -> {
            assertThat((Map<String, Map<String, Map<String, Object>>>) levels).extractingByKey(subscriptionLevel)
                .satisfies(level -> {
                  assertThat(level.get("badge")).doesNotContainKeys("duration");
                });
          });
    });
  }

  @Test
  void testGetBoostAmounts() {
    final Map<?, ?> boostAmounts = RESOURCE_EXTENSION.target("/v1/subscription/boost/amounts")
        .request()
        .get(Map.class);

    assertThat(boostAmounts).isEqualTo(Map.of(
        "USD", List.of(5.50, 6, 7, 8, 9, 10),
        "JPY", List.of(550, 600, 700, 800, 900, 1000),
        "BIF", List.of(5500, 6000, 7000, 8000, 9000, 10000)
    ));

    final Map<?, ?> giftAmounts = RESOURCE_EXTENSION.target("/v1/subscription/boost/amounts/gift")
        .request()
        .get(Map.class);

    assertThat(giftAmounts).isEqualTo(Map.of(
        "USD", 20,
        "JPY", 2000,
        "BIF", 20000
    ));
  }

  @Test
  void getLevels() {
    when(BADGE_TRANSLATOR.translate(any(), eq("B1"))).thenReturn(new Badge("B1", "cat1", "name1", "desc1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("B2"))).thenReturn(new Badge("B2", "cat2", "name2", "desc2",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(BADGE_TRANSLATOR.translate(any(), eq("B3"))).thenReturn(new Badge("B3", "cat3", "name3", "desc3",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
        List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))));
    when(LEVEL_TRANSLATOR.translate(any(), eq("B1"))).thenReturn("Z1");
    when(LEVEL_TRANSLATOR.translate(any(), eq("B2"))).thenReturn("Z2");
    when(LEVEL_TRANSLATOR.translate(any(), eq("B3"))).thenReturn("Z3");

    GetLevelsResponse response = RESOURCE_EXTENSION.target("/v1/subscription/levels")
        .request()
        .get(GetLevelsResponse.class);

    assertThat(response.getLevels()).containsKeys(5L, 15L, 35L).satisfies(longLevelMap -> {
      assertThat(longLevelMap).extractingByKey(5L).satisfies(level -> {
        assertThat(level.getName()).isEqualTo("Z1");
        assertThat(level.getBadge().getId()).isEqualTo("B1");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("5");
        });
      });
      assertThat(longLevelMap).extractingByKey(15L).satisfies(level -> {
        assertThat(level.getName()).isEqualTo("Z2");
        assertThat(level.getBadge().getId()).isEqualTo("B2");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("15");
        });
      });
      assertThat(longLevelMap).extractingByKey(35L).satisfies(level -> {
        assertThat(level.getName()).isEqualTo("Z3");
        assertThat(level.getBadge().getId()).isEqualTo("B3");
        assertThat(level.getCurrencies()).containsKeys("USD").extractingByKey("USD").satisfies(price -> {
          assertThat(price).isEqualTo("35");
        });
      });
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
        badgeGracePeriod: P15D
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
        """;

  }


}
