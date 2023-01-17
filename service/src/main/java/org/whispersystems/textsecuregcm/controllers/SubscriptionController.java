/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.stripe.exception.StripeException;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.math.BigDecimal;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.badges.LevelTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationCurrencyConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorManager;
import org.whispersystems.textsecuregcm.util.ExactlySize;

@Path("/v1/subscription")
public class SubscriptionController {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

  private final Clock clock;
  private final SubscriptionConfiguration subscriptionConfiguration;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final BadgeTranslator badgeTranslator;
  private final LevelTranslator levelTranslator;
  private final Map<String, CurrencyConfiguration> currencyConfiguration;

  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = name(SubscriptionController.class,
      "invalidAcceptLanguage");
  private static final String RECEIPT_ISSUED_COUNTER_NAME = name(SubscriptionController.class, "receiptIssued");
  private static final String PROCESSOR_TAG_NAME = "processor";
  private static final String TYPE_TAG_NAME = "type";

  public SubscriptionController(
      @Nonnull Clock clock,
      @Nonnull SubscriptionConfiguration subscriptionConfiguration,
      @Nonnull OneTimeDonationConfiguration oneTimeDonationConfiguration,
      @Nonnull SubscriptionManager subscriptionManager,
      @Nonnull StripeManager stripeManager,
      @Nonnull BraintreeManager braintreeManager,
      @Nonnull ServerZkReceiptOperations zkReceiptOperations,
      @Nonnull IssuedReceiptsManager issuedReceiptsManager,
      @Nonnull BadgeTranslator badgeTranslator,
      @Nonnull LevelTranslator levelTranslator) {
    this.clock = Objects.requireNonNull(clock);
    this.subscriptionConfiguration = Objects.requireNonNull(subscriptionConfiguration);
    this.oneTimeDonationConfiguration = Objects.requireNonNull(oneTimeDonationConfiguration);
    this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.braintreeManager = Objects.requireNonNull(braintreeManager);
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
    this.badgeTranslator = Objects.requireNonNull(badgeTranslator);
    this.levelTranslator = Objects.requireNonNull(levelTranslator);

    this.currencyConfiguration = buildCurrencyConfiguration(this.oneTimeDonationConfiguration,
        this.subscriptionConfiguration, List.of(stripeManager, braintreeManager));
  }

  private static Map<String, CurrencyConfiguration> buildCurrencyConfiguration(
      OneTimeDonationConfiguration oneTimeDonationConfiguration,
      SubscriptionConfiguration subscriptionConfiguration,
      List<SubscriptionProcessorManager> subscriptionProcessorManagers) {

    return oneTimeDonationConfiguration.currencies()
        .entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, currencyAndConfig -> {
          final String currency = currencyAndConfig.getKey();
          final OneTimeDonationCurrencyConfiguration currencyConfig = currencyAndConfig.getValue();

          final Map<String, List<BigDecimal>> oneTimeLevelsToSuggestedAmounts = Map.of(
              String.valueOf(oneTimeDonationConfiguration.boost().level()), currencyConfig.boosts(),
              String.valueOf(oneTimeDonationConfiguration.gift().level()), List.of(currencyConfig.gift())
          );

          final Map<String, BigDecimal> subscriptionLevelsToAmounts = subscriptionConfiguration.getLevels()
              .entrySet().stream()
              .filter(levelIdAndConfig -> levelIdAndConfig.getValue().getPrices().containsKey(currency))
              .collect(Collectors.toMap(
                  levelIdAndConfig -> String.valueOf(levelIdAndConfig.getKey()),
                  levelIdAndConfig -> levelIdAndConfig.getValue().getPrices().get(currency).amount()));

          final List<String> supportedPaymentMethods = Arrays.stream(PaymentMethod.values())
              .filter(paymentMethod -> subscriptionProcessorManagers.stream()
                  .anyMatch(manager -> manager.getSupportedCurrencies().contains(currency)
                      && manager.supportsPaymentMethod(paymentMethod)))
              .map(PaymentMethod::name)
              .collect(Collectors.toList());

          if (supportedPaymentMethods.isEmpty()) {
            throw new RuntimeException("Configuration has currency with no processor support: " + currency);
          }

          return new CurrencyConfiguration(currencyConfig.minimum(), oneTimeLevelsToSuggestedAmounts,
              subscriptionLevelsToAmounts, supportedPaymentMethods);
        }));
  }

  @VisibleForTesting
  GetSubscriptionConfigurationResponse buildGetSubscriptionConfigurationResponse(List<Locale> acceptableLanguages) {

    final Map<String, LevelConfiguration> levels = new HashMap<>();

    subscriptionConfiguration.getLevels().forEach((levelId, levelConfig) -> {
      final LevelConfiguration levelConfiguration = new LevelConfiguration(
          levelTranslator.translate(acceptableLanguages, levelConfig.getBadge()),
          badgeTranslator.translate(acceptableLanguages, levelConfig.getBadge()));
      levels.put(String.valueOf(levelId), levelConfiguration);
    });

    final Badge boostBadge = badgeTranslator.translate(acceptableLanguages,
        oneTimeDonationConfiguration.boost().badge());
    levels.put(String.valueOf(oneTimeDonationConfiguration.boost().level()),
        new LevelConfiguration(
            boostBadge.getName(),
            // NB: the one-time badges are PurchasableBadge, which has a `duration` field
            new PurchasableBadge(
                boostBadge,
                oneTimeDonationConfiguration.boost().expiration())));

    final Badge giftBadge = badgeTranslator.translate(acceptableLanguages, oneTimeDonationConfiguration.gift().badge());
    levels.put(String.valueOf(oneTimeDonationConfiguration.gift().level()),
        new LevelConfiguration(
            giftBadge.getName(),
            new PurchasableBadge(
                giftBadge,
                oneTimeDonationConfiguration.gift().expiration())));

    return new GetSubscriptionConfigurationResponse(currencyConfiguration, levels);
  }

  @Timed
  @DELETE
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> deleteSubscriber(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenCompose(getResult -> {
          if (getResult == GetResult.NOT_STORED || getResult == GetResult.PASSWORD_MISMATCH) {
            throw new NotFoundException();
          }
          return getResult.record.getProcessorCustomer()
                  .map(processorCustomer -> getManagerForProcessor(processorCustomer.processor()).cancelAllActiveSubscriptions(processorCustomer.customerId()))
              // a missing customer ID is OK; it means the subscriber never started to add a payment method
              .orElseGet(() -> CompletableFuture.completedFuture(null));
        })
        .thenCompose(unused -> subscriptionManager.canceledAt(requestData.subscriberUser, requestData.now))
        .thenApply(unused -> Response.ok().build());
  }

  @Timed
  @PUT
  @Path("/{subscriberId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> updateSubscriber(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenCompose(getResult -> {
          if (getResult == GetResult.PASSWORD_MISMATCH) {
            throw new ForbiddenException("subscriberId mismatch");
          } else if (getResult == GetResult.NOT_STORED) {
            // create a customer and write it to ddb
            return subscriptionManager.create(requestData.subscriberUser, requestData.hmac, requestData.now)
                .thenApply(updatedRecord -> {
                  if (updatedRecord == null) {
                    throw new ForbiddenException();
                  }
                  return updatedRecord;
                });
          } else {
            // already exists so just touch access time and return
            return subscriptionManager.accessedAt(requestData.subscriberUser, requestData.now)
                .thenApply(unused -> getResult.record);
          }
        })
        .thenApply(record -> Response.ok().build());
  }

  record CreatePaymentMethodResponse(String clientSecret, SubscriptionProcessor processor) {

  }

  @Timed
  @POST
  @Path("/{subscriberId}/create_payment_method")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPaymentMethod(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @QueryParam("type") @DefaultValue("CARD") PaymentMethod paymentMethodType) {

    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);

    final SubscriptionProcessorManager subscriptionProcessorManager = getManagerForPaymentMethod(paymentMethodType);

    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {
          final CompletableFuture<SubscriptionManager.Record> updatedRecordFuture =
              record.getProcessorCustomer()
                  .map(ProcessorCustomer::processor)
                  .map(processor -> {
                    if (processor != subscriptionProcessorManager.getProcessor()) {
                      throw new ClientErrorException("existing processor does not match", Status.CONFLICT);
                    }

                    return CompletableFuture.completedFuture(record);
                  })
                  .orElseGet(() -> subscriptionProcessorManager.createCustomer(requestData.subscriberUser)
                      .thenApply(ProcessorCustomer::customerId)
                      .thenCompose(customerId -> subscriptionManager.setProcessorAndCustomerId(record,
                          new ProcessorCustomer(customerId, subscriptionProcessorManager.getProcessor()),
                          Instant.now())));

          return updatedRecordFuture.thenCompose(
              updatedRecord -> {
                final String customerId = updatedRecord.getProcessorCustomer()
                    .filter(pc -> pc.processor().equals(subscriptionProcessorManager.getProcessor()))
                    .orElseThrow(() -> new InternalServerErrorException("record should not be missing customer"))
                    .customerId();
                return subscriptionProcessorManager.createPaymentMethodSetupToken(customerId);
              });
        })
        .thenApply(
            token -> Response.ok(new CreatePaymentMethodResponse(token, subscriptionProcessorManager.getProcessor()))
                .build());
  }

  public record CreatePayPalBillingAgreementRequest(@NotBlank String returnUrl, @NotBlank String cancelUrl) {

  }

  public record CreatePayPalBillingAgreementResponse(@NotBlank String approvalUrl, @NotBlank String token) {

  }

  @Timed
  @POST
  @Path("/{subscriberId}/create_payment_method/paypal")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPayPalPaymentMethod(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @NotNull @Valid CreatePayPalBillingAgreementRequest request,
      @Context ContainerRequestContext containerRequestContext) {

    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);

    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {

          final CompletableFuture<SubscriptionManager.Record> updatedRecordFuture =
              record.getProcessorCustomer()
                  .map(ProcessorCustomer::processor)
                  .map(processor -> {
                    if (processor != braintreeManager.getProcessor()) {
                      throw new ClientErrorException("existing processor does not match", Status.CONFLICT);
                    }
                    return CompletableFuture.completedFuture(record);
                  })
                  .orElseGet(() -> braintreeManager.createCustomer(requestData.subscriberUser)
                      .thenApply(ProcessorCustomer::customerId)
                      .thenCompose(customerId -> subscriptionManager.setProcessorAndCustomerId(record,
                          new ProcessorCustomer(customerId, braintreeManager.getProcessor()),
                          Instant.now())));

          return updatedRecordFuture.thenCompose(
              updatedRecord -> {
                final Locale locale = getAcceptableLanguagesForRequest(containerRequestContext).stream()
                    .filter(l -> !"*".equals(l.getLanguage()))
                    .findFirst()
                    .orElse(Locale.US);

                return braintreeManager.createPayPalBillingAgreement(request.returnUrl, request.cancelUrl,
                    locale.toLanguageTag());
              });
        })
        .thenApply(
            billingAgreementApprovalDetails -> Response.ok(
                    new CreatePayPalBillingAgreementResponse(billingAgreementApprovalDetails.approvalUrl(),
                        billingAgreementApprovalDetails.billingAgreementToken()))
                .build());
  }

  private SubscriptionProcessorManager getManagerForPaymentMethod(PaymentMethod paymentMethod) {
    return switch (paymentMethod) {
      case CARD -> stripeManager;
      case PAYPAL -> braintreeManager;
    };
  }

  private SubscriptionProcessorManager getManagerForProcessor(SubscriptionProcessor processor) {
    return switch (processor) {
      case STRIPE -> stripeManager;
      case BRAINTREE -> braintreeManager;
    };
  }

  @Timed
  @POST
  @Path("/{subscriberId}/default_payment_method/{paymentMethodId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // use /{subscriberId}/default_payment_method/{processor}/{paymentMethodId}
  public CompletableFuture<Response> setDefaultPaymentMethod(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("paymentMethodId") @NotEmpty String paymentMethodId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> stripeManager.setDefaultPaymentMethodForCustomer(
            record.getProcessorCustomer().orElseThrow().customerId(), paymentMethodId, record.subscriptionId))
        .thenApply(customer -> Response.ok().build());
  }

  @Timed
  @POST
  @Path("/{subscriberId}/default_payment_method/{processor}/{paymentMethodToken}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethodWithProcessor(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("processor") SubscriptionProcessor processor,
      @PathParam("paymentMethodToken") @NotEmpty String paymentMethodToken) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);

    final SubscriptionProcessorManager manager = getManagerForProcessor(processor);

    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> record.getProcessorCustomer()
            .map(processorCustomer -> manager.setDefaultPaymentMethodForCustomer(processorCustomer.customerId(),
                paymentMethodToken, record.subscriptionId))
            .orElseThrow(() ->
                // a missing customer ID indicates the client made requests out of order,
                // and needs to call create_payment_method to create a customer for the given payment method
                new ClientErrorException(Status.CONFLICT)))
        .thenApply(customer -> Response.ok().build());
  }
  public static class SetSubscriptionLevelSuccessResponse {

    private final long level;

    @JsonCreator
    public SetSubscriptionLevelSuccessResponse(
        @JsonProperty("level") long level) {
      this.level = level;
    }

    public long getLevel() {
      return level;
    }
  }

  public static class SetSubscriptionLevelErrorResponse {

    public static class Error {

      public enum Type {
        UNSUPPORTED_LEVEL,
        UNSUPPORTED_CURRENCY,
        PAYMENT_REQUIRES_ACTION,
      }

      private final Type type;
      private final String message;

      @JsonCreator
      public Error(
          @JsonProperty("type") Type type,
          @JsonProperty("message") String message) {
        this.type = type;
        this.message = message;
      }

      public Type getType() {
        return type;
      }

      public String getMessage() {
        return message;
      }
    }

    private final List<Error> errors;

    @JsonCreator
    public SetSubscriptionLevelErrorResponse(
        @JsonProperty("errors") List<Error> errors) {
      this.errors = errors;
    }

    public List<Error> getErrors() {
      return errors;
    }
  }

  @Timed
  @PUT
  @Path("/{subscriberId}/level/{level}/{currency}/{idempotencyKey}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setSubscriptionLevel(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("level") long level,
      @PathParam("currency") String currency,
      @PathParam("idempotencyKey") String idempotencyKey) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {

            final ProcessorCustomer processorCustomer = record.getProcessorCustomer()
                    .orElseThrow(() ->
                            // a missing customer ID indicates the client made requests out of order,
                            // and needs to call create_payment_method to create a customer for the given payment method
                            new ClientErrorException(Status.CONFLICT));

            final String subscriptionTemplateId = getSubscriptionTemplateId(level, currency, processorCustomer.processor());

            final SubscriptionProcessorManager manager = getManagerForProcessor(processorCustomer.processor());

            return Optional.ofNullable(record.subscriptionId)
                    .map(subId -> {
                        // we already have a subscription in our records so let's check the level and change it if needed
                        return manager.getSubscription(subId).thenCompose(
                                subscription -> manager.getLevelForSubscription(subscription).thenCompose(existingLevel -> {
                                    if (level == existingLevel) {
                                        return CompletableFuture.completedFuture(subscription);
                              }
                              return manager.updateSubscription(
                                              subscription, subscriptionTemplateId, level, idempotencyKey)
                                      .thenCompose(updatedSubscription ->
                                              subscriptionManager.subscriptionLevelChanged(requestData.subscriberUser,
                                                      requestData.now,
                                                      level, updatedSubscription.id())
                                                      .thenApply(unused -> updatedSubscription));
                          }));
              }).orElseGet(() -> {
                      long lastSubscriptionCreatedAt =
                              record.subscriptionCreatedAt != null ? record.subscriptionCreatedAt.getEpochSecond() : 0;

                      // we don't have a subscription yet so create it and then record the subscription id
                      //
                      // this relies on stripe's idempotency key to avoid creating more than one subscription if the client
                      // retries this request
                      return manager.createSubscription(processorCustomer.customerId(),
                                      subscriptionTemplateId,
                                      level,
                                      lastSubscriptionCreatedAt)
                              .exceptionally(e -> {
                                  if (e.getCause() instanceof StripeException stripeException
                                          && stripeException.getCode().equals("subscription_payment_intent_requires_action")) {
                                      throw new BadRequestException(Response.status(Status.BAD_REQUEST)
                                              .entity(new SetSubscriptionLevelErrorResponse(List.of(
                                                      new SetSubscriptionLevelErrorResponse.Error(
                                                              SetSubscriptionLevelErrorResponse.Error.Type.PAYMENT_REQUIRES_ACTION, null
                                )
                            ))).build());
                      }
                        if (e instanceof RuntimeException re) {
                            throw re;
                        }

                        throw new CompletionException(e);
                    })
                        .thenCompose(subscription -> subscriptionManager.subscriptionCreated(
                                        requestData.subscriberUser, subscription.id(), requestData.now, level)
                                .thenApply(unused -> subscription));
                  });
        })
            .thenApply(unused -> Response.ok(new SetSubscriptionLevelSuccessResponse(level)).build());
  }

  public static class GetLevelsResponse {

    public static class Level {

      private final String name;
      private final Badge badge;
      private final Map<String, BigDecimal> currencies;

      @JsonCreator
      public Level(
          @JsonProperty("name") String name,
          @JsonProperty("badge") Badge badge,
          @JsonProperty("currencies") Map<String, BigDecimal> currencies) {
        this.name = name;
        this.badge = badge;
        this.currencies = currencies;
      }

      public String getName() {
        return name;
      }

      public Badge getBadge() {
        return badge;
      }

      public Map<String, BigDecimal> getCurrencies() {
        return currencies;
      }
    }

    private final Map<Long, Level> levels;

    @JsonCreator
    public GetLevelsResponse(
        @JsonProperty("levels") Map<Long, Level> levels) {
      this.levels = levels;
    }

    public Map<Long, Level> getLevels() {
      return levels;
    }
  }

  /**
   * Comprehensive configuration for subscriptions and one-time donations
   *
   * @param currencies map of lower-cased ISO 3 currency codes to minimums and level-specific scalar amounts
   * @param levels     map of numeric level IDs to level-specific configuration
   */
  public record GetSubscriptionConfigurationResponse(Map<String, CurrencyConfiguration> currencies,
                                                     Map<String, LevelConfiguration> levels) {

  }

  /**
   * Configuration for a currency - use to present appropriate client interfaces
   *
   * @param minimum                 the minimum amount that may be submitted for a one-time donation in the currency
   * @param oneTime                 map of numeric one-time donation level IDs to the list of default amounts to be
   *                                presented
   * @param subscription            map of numeric subscription level IDs to the amount charged for that level
   * @param supportedPaymentMethods the payment methods that support the given currency
   */
  public record CurrencyConfiguration(BigDecimal minimum, Map<String, List<BigDecimal>> oneTime,
                                      Map<String, BigDecimal> subscription,
                                      List<String> supportedPaymentMethods) {

  }

  /**
   * Configuration for a donation level - use to present appropriate client interfaces
   *
   * @param name  the localized name for the level
   * @param badge the displayable badge associated with the level
   */
  public record LevelConfiguration(String name, Badge badge) {

  }

  @Timed
  @GET
  @Path("/configuration")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getConfiguration(@Context ContainerRequestContext containerRequestContext) {
    return CompletableFuture.supplyAsync(() -> {
      List<Locale> acceptableLanguages = getAcceptableLanguagesForRequest(containerRequestContext);
      return Response.ok(buildGetSubscriptionConfigurationResponse(acceptableLanguages)).build();
    });
  }

  @Timed
  @GET
  @Path("/levels")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // use /configuration
  public CompletableFuture<Response> getLevels(@Context ContainerRequestContext containerRequestContext) {
    return CompletableFuture.supplyAsync(() -> {
      List<Locale> acceptableLanguages = getAcceptableLanguagesForRequest(containerRequestContext);
      GetLevelsResponse getLevelsResponse = new GetLevelsResponse(
          subscriptionConfiguration.getLevels().entrySet().stream().collect(Collectors.toMap(Entry::getKey,
              entry -> new GetLevelsResponse.Level(
                  levelTranslator.translate(acceptableLanguages, entry.getValue().getBadge()),
                  badgeTranslator.translate(acceptableLanguages, entry.getValue().getBadge()),
                  entry.getValue().getPrices().entrySet().stream().collect(
                      Collectors.toMap(levelEntry -> levelEntry.getKey().toUpperCase(Locale.ROOT),
                          levelEntry -> levelEntry.getValue().amount()))))));
      return Response.ok(getLevelsResponse).build();
    });
  }

  public static class GetBoostBadgesResponse {
    public static class Level {
      private final PurchasableBadge badge;

      @JsonCreator
      public Level(
          @JsonProperty("badge") PurchasableBadge badge) {
        this.badge = badge;
      }

      public PurchasableBadge getBadge() {
        return badge;
      }
    }

    private final Map<Long, Level> levels;

    @JsonCreator
    public GetBoostBadgesResponse(
        @JsonProperty("levels") Map<Long, Level> levels) {
      this.levels = Objects.requireNonNull(levels);
    }

    public Map<Long, Level> getLevels() {
      return levels;
    }
  }

  @Timed
  @GET
  @Path("/boost/badges")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // use /configuration
  public CompletableFuture<Response> getBoostBadges(@Context ContainerRequestContext containerRequestContext) {
    return CompletableFuture.supplyAsync(() -> {
      long boostLevel = oneTimeDonationConfiguration.boost().level();
      String boostBadge = oneTimeDonationConfiguration.boost().badge();
      long giftLevel = oneTimeDonationConfiguration.gift().level();
      String giftBadge = oneTimeDonationConfiguration.gift().badge();
      List<Locale> acceptableLanguages = getAcceptableLanguagesForRequest(containerRequestContext);
      GetBoostBadgesResponse getBoostBadgesResponse = new GetBoostBadgesResponse(Map.of(
          boostLevel, new GetBoostBadgesResponse.Level(
              new PurchasableBadge(badgeTranslator.translate(acceptableLanguages, boostBadge),
                  oneTimeDonationConfiguration.boost().expiration())),
          giftLevel, new GetBoostBadgesResponse.Level(
              new PurchasableBadge(badgeTranslator.translate(acceptableLanguages, giftBadge),
                  oneTimeDonationConfiguration.gift().expiration()))));
      return Response.ok(getBoostBadgesResponse).build();
    });
  }

  @Timed
  @GET
  @Path("/boost/amounts")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // use /configuration
  public CompletableFuture<Response> getBoostAmounts() {
    return CompletableFuture.supplyAsync(() -> Response.ok(
            oneTimeDonationConfiguration.currencies().entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().toUpperCase(Locale.ROOT), entry -> entry.getValue().boosts())))
        .build());
  }

  @Timed
  @GET
  @Path("/boost/amounts/gift")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // use /configuration
  public CompletableFuture<Response> getGiftAmounts() {
    return CompletableFuture.supplyAsync(() -> Response.ok(
            oneTimeDonationConfiguration.currencies().entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().toUpperCase(Locale.ROOT), entry -> entry.getValue().gift())))
        .build());
  }

  public static class CreateBoostRequest {

    @NotEmpty
    @ExactlySize(3)
    public String currency;
    @Min(1)
    public long amount;
    public Long level;
  }

  public static class CreatePayPalBoostRequest extends CreateBoostRequest {

    @NotEmpty
    public String returnUrl;
    @NotEmpty
    public String cancelUrl;
  }

  record CreatePayPalBoostResponse(String approvalUrl, String paymentId) {

  }

  public static class CreateBoostResponse {

    private final String clientSecret;

    @JsonCreator
    public CreateBoostResponse(
        @JsonProperty("clientSecret") String clientSecret) {
      this.clientSecret = clientSecret;
    }

    public String getClientSecret() {
      return clientSecret;
    }
  }

  /**
   * Creates a Stripe PaymentIntent with the requested amount and currency
   */
  @Timed
  @POST
  @Path("/boost/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createBoostPaymentIntent(@NotNull @Valid CreateBoostRequest request) {
    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }
          BigDecimal amount = BigDecimal.valueOf(request.amount);
          if (request.level == oneTimeDonationConfiguration.gift().level()) {
            BigDecimal amountConfigured = oneTimeDonationConfiguration.currencies()
                .get(request.currency.toLowerCase(Locale.ROOT)).gift();
            if (amountConfigured == null ||
                SubscriptionCurrencyUtil.convertConfiguredAmountToStripeAmount(request.currency, amountConfigured)
                    .compareTo(amount) != 0) {
              throw new WebApplicationException(
                  Response.status(Status.CONFLICT).entity(Map.of("error", "level_amount_mismatch")).build());
            }
          }
          validateRequestCurrencyAmount(request, amount, stripeManager);
        })
        .thenCompose(unused -> stripeManager.createPaymentIntent(request.currency, request.amount, request.level))
        .thenApply(paymentIntent -> Response.ok(new CreateBoostResponse(paymentIntent.getClientSecret())).build());
  }

  /**
   * Validates that the currency and amount in the request are supported by the {@code manager} and exceed the minimum
   * permitted amount
   *
   * @throws BadRequestException indicates validation failed. Inspect {@code response.error} for details
   */
  private void validateRequestCurrencyAmount(CreateBoostRequest request, BigDecimal amount,
      SubscriptionProcessorManager manager) {

    if (!manager.supportsCurrency(request.currency.toLowerCase(Locale.ROOT))) {
      throw new BadRequestException(Response.status(Status.BAD_REQUEST)
          .entity(Map.of("error", "unsupported_currency")).build());
    }

    BigDecimal minCurrencyAmountMajorUnits = oneTimeDonationConfiguration.currencies()
        .get(request.currency.toLowerCase(Locale.ROOT)).minimum();
    BigDecimal minCurrencyAmountMinorUnits = SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(
        request.currency,
        minCurrencyAmountMajorUnits);
    if (minCurrencyAmountMinorUnits.compareTo(amount) > 0) {
      throw new BadRequestException(Response.status(Status.BAD_REQUEST)
          .entity(Map.of(
              "error", "amount_below_currency_minimum",
              "minimum", minCurrencyAmountMajorUnits.toString())).build());
    }
  }

  @Timed
  @POST
  @Path("/boost/paypal/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPayPalBoost(@NotNull @Valid CreatePayPalBoostRequest request,
      @Context ContainerRequestContext containerRequestContext) {

    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }

          validateRequestCurrencyAmount(request, BigDecimal.valueOf(request.amount), braintreeManager);
        })
        .thenCompose(unused -> {
          final Locale locale = getAcceptableLanguagesForRequest(containerRequestContext).stream()
              .filter(l -> !"*".equals(l.getLanguage()))
              .findFirst()
              .orElse(Locale.US);

          return braintreeManager.createOneTimePayment(request.currency.toUpperCase(Locale.ROOT), request.amount,
              locale.toLanguageTag(),
              request.returnUrl, request.cancelUrl);
        })
        .thenApply(approvalDetails -> Response.ok(
            new CreatePayPalBoostResponse(approvalDetails.approvalUrl(), approvalDetails.paymentId())).build());
  }

  public static class ConfirmPayPalBoostRequest extends CreateBoostRequest {

    @NotEmpty
    public String payerId;
    @NotEmpty
    public String paymentId; // PAYID-…
    @NotEmpty
    public String paymentToken; // EC-…
  }

  record ConfirmPayPalBoostResponse(String paymentId) {

  }

  @Timed
  @POST
  @Path("/boost/paypal/confirm")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> confirmPayPalBoost(@NotNull @Valid ConfirmPayPalBoostRequest request) {

    return CompletableFuture.runAsync(() -> {
          if (request.level == null) {
            request.level = oneTimeDonationConfiguration.boost().level();
          }
        })
        .thenCompose(unused -> braintreeManager.captureOneTimePayment(request.payerId, request.paymentId,
            request.paymentToken, request.currency, request.amount, request.level))
        .thenApply(chargeSuccessDetails -> Response.ok(
            new ConfirmPayPalBoostResponse(chargeSuccessDetails.paymentId())).build());
  }

  public static class CreateBoostReceiptCredentialsRequest {

    /**
     * a payment ID from {@link #processor}
     */
    @NotNull
    public String paymentIntentId;
    @NotNull
    public byte[] receiptCredentialRequest;

    @NotNull
    public SubscriptionProcessor processor = SubscriptionProcessor.STRIPE;
  }

  public static class CreateBoostReceiptCredentialsResponse {

    private final byte[] receiptCredentialResponse;

    @JsonCreator
    public CreateBoostReceiptCredentialsResponse(
        @JsonProperty("receiptCredentialResponse") byte[] receiptCredentialResponse) {
      this.receiptCredentialResponse = receiptCredentialResponse;
    }

    public byte[] getReceiptCredentialResponse() {
      return receiptCredentialResponse;
    }
  }

  @Timed
  @POST
  @Path("/boost/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createBoostReceiptCredentials(@NotNull @Valid CreateBoostReceiptCredentialsRequest request) {

    final SubscriptionProcessorManager manager = getManagerForProcessor(request.processor);

    return manager.getPaymentDetails(request.paymentIntentId)
        .thenCompose(paymentDetails -> {
          if (paymentDetails == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
          }
          switch (paymentDetails.status()) {
            case PROCESSING -> throw new WebApplicationException(Status.NO_CONTENT);
            case SUCCEEDED -> {
            }
            default -> throw new WebApplicationException(Status.PAYMENT_REQUIRED);
          }

          long level = oneTimeDonationConfiguration.boost().level();
          if (paymentDetails.customMetadata() != null) {
            String levelMetadata = paymentDetails.customMetadata()
                .getOrDefault("level", Long.toString(oneTimeDonationConfiguration.boost().level()));
            try {
              level = Long.parseLong(levelMetadata);
            } catch (NumberFormatException e) {
              logger.error("failed to parse level metadata ({}) on payment intent {}", levelMetadata,
                  paymentDetails.id(), e);
              throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
            }
          }
          Duration levelExpiration;
          if (oneTimeDonationConfiguration.boost().level() == level) {
            levelExpiration = oneTimeDonationConfiguration.boost().expiration();
          } else if (oneTimeDonationConfiguration.gift().level() == level) {
            levelExpiration = oneTimeDonationConfiguration.gift().expiration();
          } else {
            logger.error("level ({}) returned from payment intent that is unknown to the server", level);
            throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
          }
          ReceiptCredentialRequest receiptCredentialRequest;
          try {
            receiptCredentialRequest = new ReceiptCredentialRequest(request.receiptCredentialRequest);
          } catch (InvalidInputException e) {
            throw new BadRequestException("invalid receipt credential request", e);
          }
          final long finalLevel = level;
          return issuedReceiptsManager.recordIssuance(paymentDetails.id(), manager.getProcessor(),
                  receiptCredentialRequest, clock.instant())
              .thenApply(unused -> {
                Instant expiration = paymentDetails.created()
                    .plus(levelExpiration)
                    .truncatedTo(ChronoUnit.DAYS)
                    .plus(1, ChronoUnit.DAYS);
                ReceiptCredentialResponse receiptCredentialResponse;
                try {
                  receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                      receiptCredentialRequest, expiration.getEpochSecond(), finalLevel);
                } catch (VerificationFailedException e) {
                  throw new BadRequestException("receipt credential request failed verification", e);
                }
                Metrics.counter(RECEIPT_ISSUED_COUNTER_NAME,
                        PROCESSOR_TAG_NAME, manager.getProcessor().toString(),
                        TYPE_TAG_NAME, "boost")
                    .increment();
                return Response.ok(new CreateBoostReceiptCredentialsResponse(receiptCredentialResponse.serialize()))
                    .build();
              });
        });
  }

  public static class GetSubscriptionInformationResponse {

    public static class Subscription {

      private final long level;
      private final Instant billingCycleAnchor;
      private final Instant endOfCurrentPeriod;
      private final boolean active;
      private final boolean cancelAtPeriodEnd;
      private final String currency;
      private final BigDecimal amount;
      private final String status;
      private final SubscriptionProcessor processor;

      @JsonCreator
      public Subscription(
          @JsonProperty("level") long level,
          @JsonProperty("billingCycleAnchor") Instant billingCycleAnchor,
          @JsonProperty("endOfCurrentPeriod") Instant endOfCurrentPeriod,
          @JsonProperty("active") boolean active,
          @JsonProperty("cancelAtPeriodEnd") boolean cancelAtPeriodEnd,
          @JsonProperty("currency") String currency,
          @JsonProperty("amount") BigDecimal amount,
          @JsonProperty("status") String status,
          @JsonProperty("processor") SubscriptionProcessor processor) {
        this.level = level;
        this.billingCycleAnchor = billingCycleAnchor;
        this.endOfCurrentPeriod = endOfCurrentPeriod;
        this.active = active;
        this.cancelAtPeriodEnd = cancelAtPeriodEnd;
        this.currency = currency;
        this.amount = amount;
        this.status = status;
        this.processor = processor;
      }

      public long getLevel() {
        return level;
      }

      public Instant getBillingCycleAnchor() {
        return billingCycleAnchor;
      }

      public Instant getEndOfCurrentPeriod() {
        return endOfCurrentPeriod;
      }

      public boolean isActive() {
        return active;
      }

      public boolean isCancelAtPeriodEnd() {
        return cancelAtPeriodEnd;
      }

      public String getCurrency() {
        return currency;
      }

      public BigDecimal getAmount() {
        return amount;
      }

      public String getStatus() {
        return status;
      }

      public SubscriptionProcessor getProcessor() {
        return processor;
      }
    }

    public static class ChargeFailure {
      private final String code;
      private final String message;
      private final String outcomeNetworkStatus;
      private final String outcomeReason;
      private final String outcomeType;

      @JsonCreator
      public ChargeFailure(
          @JsonProperty("code") String code,
          @JsonProperty("message") String message,
          @JsonProperty("outcomeNetworkStatus") String outcomeNetworkStatus,
          @JsonProperty("outcomeReason") String outcomeReason,
          @JsonProperty("outcomeType") String outcomeType) {
        this.code = code;
        this.message = message;
        this.outcomeNetworkStatus = outcomeNetworkStatus;
        this.outcomeReason = outcomeReason;
        this.outcomeType = outcomeType;
      }

      public String getCode() {
        return code;
      }

      public String getMessage() {
        return message;
      }

      public String getOutcomeNetworkStatus() {
        return outcomeNetworkStatus;
      }

      public String getOutcomeReason() {
        return outcomeReason;
      }

      public String getOutcomeType() {
        return outcomeType;
      }
    }

    private final Subscription subscription;
    private final ChargeFailure chargeFailure;

    @JsonCreator
    public GetSubscriptionInformationResponse(
        @JsonProperty("subscription") Subscription subscription,
        @JsonProperty("chargeFailure") ChargeFailure chargeFailure) {
      this.subscription = subscription;
      this.chargeFailure = chargeFailure;
    }

    public Subscription getSubscription() {
      return subscription;
    }

    @JsonInclude(Include.NON_NULL)
    public ChargeFailure getChargeFailure() {
      return chargeFailure;
    }
  }

  @Timed
  @GET
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getSubscriptionInformation(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {
            if (record.subscriptionId == null) {
                return CompletableFuture.completedFuture(Response.ok(new GetSubscriptionInformationResponse(null, null)).build());
            }

            final SubscriptionProcessorManager manager = getManagerForProcessor(record.getProcessorCustomer().orElseThrow().processor());

            return manager.getSubscription(record.subscriptionId).thenCompose(subscription ->
                    manager.getSubscriptionInformation(subscription).thenApply(subscriptionInformation -> {
                        final GetSubscriptionInformationResponse.ChargeFailure chargeFailure = Optional.ofNullable(subscriptionInformation.chargeFailure())
                                .map(chargeFailure1 -> new GetSubscriptionInformationResponse.ChargeFailure(
                                        subscriptionInformation.chargeFailure().code(),
                                        subscriptionInformation.chargeFailure().message(),
                                        subscriptionInformation.chargeFailure().outcomeNetworkStatus(),
                                        subscriptionInformation.chargeFailure().outcomeReason(),
                                        subscriptionInformation.chargeFailure().outcomeType()
                                ))
                                .orElse(null);
                        return Response.ok(
                                new GetSubscriptionInformationResponse(
                                    new GetSubscriptionInformationResponse.Subscription(
                                        subscriptionInformation.level(),
                                        subscriptionInformation.billingCycleAnchor(),
                                        subscriptionInformation.endOfCurrentPeriod(),
                                        subscriptionInformation.active(),
                                        subscriptionInformation.cancelAtPeriodEnd(),
                                        subscriptionInformation.price().currency(),
                                        subscriptionInformation.price().amount(),
                                        subscriptionInformation.status().getApiValue(),
                                        manager.getProcessor()),
                                    chargeFailure
                                )).build();
                    }));
        });
  }

  public static class GetReceiptCredentialsRequest {

    private final byte[] receiptCredentialRequest;

    @JsonCreator
    public GetReceiptCredentialsRequest(
        @JsonProperty("receiptCredentialRequest") byte[] receiptCredentialRequest) {
      this.receiptCredentialRequest = receiptCredentialRequest;
    }

    @NotEmpty
    public byte[] getReceiptCredentialRequest() {
      return receiptCredentialRequest;
    }
  }

  public static class GetReceiptCredentialsResponse {

    private final byte[] receiptCredentialResponse;

    @JsonCreator
    public GetReceiptCredentialsResponse(
        @JsonProperty("receiptCredentialResponse") byte[] receiptCredentialResponse) {
      this.receiptCredentialResponse = receiptCredentialResponse;
    }

    @NotEmpty
    public byte[] getReceiptCredentialResponse() {
      return receiptCredentialResponse;
    }
  }

  @Timed
  @POST
  @Path("/{subscriberId}/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createSubscriptionReceiptCredentials(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @NotNull @Valid GetReceiptCredentialsRequest request) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {
            if (record.subscriptionId == null) {
                return CompletableFuture.completedFuture(Response.status(Status.NOT_FOUND).build());
            }
            ReceiptCredentialRequest receiptCredentialRequest;
            try {
                receiptCredentialRequest = new ReceiptCredentialRequest(request.getReceiptCredentialRequest());
            } catch (InvalidInputException e) {
                throw new BadRequestException("invalid receipt credential request", e);
            }

            final SubscriptionProcessorManager manager = getManagerForProcessor(record.getProcessorCustomer().orElseThrow().processor());
            return manager.getReceiptItem(record.subscriptionId)
                    .thenCompose(receipt -> issuedReceiptsManager.recordIssuance(
                            receipt.itemId(), manager.getProcessor(), receiptCredentialRequest,
                            requestData.now)
                            .thenApply(unused -> receipt))
                    .thenApply(receipt -> {
                      ReceiptCredentialResponse receiptCredentialResponse;
                      try {
                        receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                            receiptCredentialRequest,
                            receiptExpirationWithGracePeriod(receipt.expiration()).getEpochSecond(), receipt.level());
                      } catch (VerificationFailedException e) {
                        throw new BadRequestException("receipt credential request failed verification", e);
                      }
                      Metrics.counter(RECEIPT_ISSUED_COUNTER_NAME,
                              PROCESSOR_TAG_NAME, manager.getProcessor().toString(),
                              TYPE_TAG_NAME, "subscription")
                          .increment();
                      return Response.ok(new GetReceiptCredentialsResponse(receiptCredentialResponse.serialize()))
                          .build();
                    });
        });
  }

    private Instant receiptExpirationWithGracePeriod(Instant itemExpiration) {
        return itemExpiration.plus(subscriptionConfiguration.getBadgeGracePeriod())
                .truncatedTo(ChronoUnit.DAYS)
                .plus(1, ChronoUnit.DAYS);
    }

    private String getSubscriptionTemplateId(long level, String currency, SubscriptionProcessor processor) {
      SubscriptionLevelConfiguration levelConfiguration = subscriptionConfiguration.getLevels().get(level);
      if (levelConfiguration == null) {
        throw new BadRequestException(Response.status(Status.BAD_REQUEST)
            .entity(new SetSubscriptionLevelErrorResponse(List.of(
                new SetSubscriptionLevelErrorResponse.Error(
                    SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_LEVEL, null))))
            .build());
      }

      return Optional.ofNullable(levelConfiguration.getPrices()
              .get(currency.toLowerCase(Locale.ROOT)))
          .map(priceConfiguration -> priceConfiguration.processorIds().get(processor))
          .orElseThrow(() -> new BadRequestException(Response.status(Status.BAD_REQUEST)
              .entity(new SetSubscriptionLevelErrorResponse(List.of(
                  new SetSubscriptionLevelErrorResponse.Error(
                      SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_CURRENCY, null))))
              .build()));
    }

  private SubscriptionManager.Record requireRecordFromGetResult(SubscriptionManager.GetResult getResult) {
    if (getResult == GetResult.PASSWORD_MISMATCH) {
      throw new ForbiddenException("subscriberId mismatch");
    } else if (getResult == GetResult.NOT_STORED) {
      throw new NotFoundException();
    } else {
      return getResult.record;
    }
  }

  private List<Locale> getAcceptableLanguagesForRequest(ContainerRequestContext containerRequestContext) {
    try {
      return containerRequestContext.getAcceptableLanguages();
    } catch (final ProcessingException e) {
      final String userAgent = containerRequestContext.getHeaderString(HttpHeaders.USER_AGENT);
      Metrics.counter(INVALID_ACCEPT_LANGUAGE_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();
      logger.debug("Could not get acceptable languages; Accept-Language: {}; User-Agent: {}",
          containerRequestContext.getHeaderString(HttpHeaders.ACCEPT_LANGUAGE),
          userAgent,
          e);

      return List.of();
    }
  }

  private static class RequestData {

    public final byte[] subscriberBytes;
    public final byte[] subscriberUser;
    public final byte[] subscriberKey;
    public final byte[] hmac;
    public final Instant now;

    private RequestData(
        @Nonnull byte[] subscriberBytes,
        @Nonnull byte[] subscriberUser,
        @Nonnull byte[] subscriberKey,
        @Nonnull byte[] hmac,
        @Nonnull Instant now) {
      this.subscriberBytes = Objects.requireNonNull(subscriberBytes);
      this.subscriberUser = Objects.requireNonNull(subscriberUser);
      this.subscriberKey = Objects.requireNonNull(subscriberKey);
      this.hmac = Objects.requireNonNull(hmac);
      this.now = Objects.requireNonNull(now);
    }

    public static RequestData process(
        Optional<AuthenticatedAccount> authenticatedAccount,
        String subscriberId,
        Clock clock) {
      Instant now = clock.instant();
      if (authenticatedAccount.isPresent()) {
        throw new ForbiddenException("must not use authenticated connection for subscriber operations");
      }
      byte[] subscriberBytes = convertSubscriberIdStringToBytes(subscriberId);
      byte[] subscriberUser = getUser(subscriberBytes);
      byte[] subscriberKey = getKey(subscriberBytes);
      byte[] hmac = computeHmac(subscriberUser, subscriberKey);
      return new RequestData(subscriberBytes, subscriberUser, subscriberKey, hmac, now);
    }

    private static byte[] convertSubscriberIdStringToBytes(String subscriberId) {
      try {
        byte[] bytes = Base64.getUrlDecoder().decode(subscriberId);
        if (bytes.length != 32) {
          throw new NotFoundException();
        }
        return bytes;
      } catch (IllegalArgumentException e) {
        throw new NotFoundException(e);
      }
    }

    private static byte[] getUser(byte[] subscriberBytes) {
      byte[] user = new byte[16];
      System.arraycopy(subscriberBytes, 0, user, 0, user.length);
      return user;
    }

    private static byte[] getKey(byte[] subscriberBytes) {
      byte[] key = new byte[16];
      System.arraycopy(subscriberBytes, 16, key, 0, key.length);
      return key;
    }

    private static byte[] computeHmac(byte[] subscriberUser, byte[] subscriberKey) {
      try {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(subscriberKey, "HmacSHA256"));
        return mac.doFinal(subscriberUser);
      } catch (NoSuchAlgorithmException | InvalidKeyException e) {
        throw new InternalServerErrorException(e);
      }
    }
  }
}
