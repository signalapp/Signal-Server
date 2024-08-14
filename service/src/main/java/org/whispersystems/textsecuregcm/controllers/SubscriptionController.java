/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.badges.LevelTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationCurrencyConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.SubscriberCredentials;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BankTransferType;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/subscription")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Subscriptions")
public class SubscriptionController {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

  private final Clock clock;
  private final SubscriptionConfiguration subscriptionConfiguration;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final BadgeTranslator badgeTranslator;
  private final LevelTranslator levelTranslator;
  private final BankMandateTranslator bankMandateTranslator;
  static final String RECEIPT_ISSUED_COUNTER_NAME = MetricsUtil.name(SubscriptionController.class, "receiptIssued");
  static final String PROCESSOR_TAG_NAME = "processor";
  static final String TYPE_TAG_NAME = "type";
  private static final String SUBSCRIPTION_TYPE_TAG_NAME = "subscriptionType";

  public SubscriptionController(
      @Nonnull Clock clock,
      @Nonnull SubscriptionConfiguration subscriptionConfiguration,
      @Nonnull OneTimeDonationConfiguration oneTimeDonationConfiguration,
      @Nonnull SubscriptionManager subscriptionManager,
      @Nonnull StripeManager stripeManager,
      @Nonnull BraintreeManager braintreeManager,
      @Nonnull BadgeTranslator badgeTranslator,
      @Nonnull LevelTranslator levelTranslator,
      @Nonnull BankMandateTranslator bankMandateTranslator) {
    this.subscriptionManager = subscriptionManager;
    this.clock = Objects.requireNonNull(clock);
    this.subscriptionConfiguration = Objects.requireNonNull(subscriptionConfiguration);
    this.oneTimeDonationConfiguration = Objects.requireNonNull(oneTimeDonationConfiguration);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.braintreeManager = Objects.requireNonNull(braintreeManager);
    this.badgeTranslator = Objects.requireNonNull(badgeTranslator);
    this.levelTranslator = Objects.requireNonNull(levelTranslator);
    this.bankMandateTranslator = Objects.requireNonNull(bankMandateTranslator);
  }

  private Map<String, CurrencyConfiguration> buildCurrencyConfiguration() {
    final List<SubscriptionPaymentProcessor> subscriptionPaymentProcessors = List.of(stripeManager, braintreeManager);
    return oneTimeDonationConfiguration.currencies()
        .entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, currencyAndConfig -> {
          final String currency = currencyAndConfig.getKey();
          final OneTimeDonationCurrencyConfiguration currencyConfig = currencyAndConfig.getValue();

          final Map<String, List<BigDecimal>> oneTimeLevelsToSuggestedAmounts = Map.of(
              String.valueOf(oneTimeDonationConfiguration.boost().level()), currencyConfig.boosts(),
              String.valueOf(oneTimeDonationConfiguration.gift().level()), List.of(currencyConfig.gift())
          );

          final Function<Map<Long, ? extends SubscriptionLevelConfiguration>, Map<String, BigDecimal>> extractSubscriptionAmounts = levels ->
              levels.entrySet().stream()
                  .filter(levelIdAndConfig -> levelIdAndConfig.getValue().prices().containsKey(currency))
                  .collect(Collectors.toMap(
                      levelIdAndConfig -> String.valueOf(levelIdAndConfig.getKey()),
                      levelIdAndConfig -> levelIdAndConfig.getValue().prices().get(currency).amount()));

          final List<String> supportedPaymentMethods = Arrays.stream(PaymentMethod.values())
              .filter(paymentMethod -> subscriptionPaymentProcessors.stream()
                  .anyMatch(manager -> manager.supportsPaymentMethod(paymentMethod)
                      && manager.getSupportedCurrenciesForPaymentMethod(paymentMethod).contains(currency)))
              .map(PaymentMethod::name)
              .collect(Collectors.toList());

          if (supportedPaymentMethods.isEmpty()) {
            throw new RuntimeException("Configuration has currency with no processor support: " + currency);
          }

          return new CurrencyConfiguration(
              currencyConfig.minimum(),
              oneTimeLevelsToSuggestedAmounts,
              extractSubscriptionAmounts.apply(subscriptionConfiguration.getDonationLevels()),
              extractSubscriptionAmounts.apply(subscriptionConfiguration.getBackupLevels()),
              supportedPaymentMethods);
        }));
  }

  @VisibleForTesting
  GetSubscriptionConfigurationResponse buildGetSubscriptionConfigurationResponse(
      final List<Locale> acceptableLanguages) {
    final Map<String, LevelConfiguration> donationLevels = new HashMap<>();

    subscriptionConfiguration.getDonationLevels().forEach((levelId, levelConfig) -> {
      final LevelConfiguration levelConfiguration = new LevelConfiguration(
          levelTranslator.translate(acceptableLanguages, levelConfig.badge()),
          badgeTranslator.translate(acceptableLanguages, levelConfig.badge()));
      donationLevels.put(String.valueOf(levelId), levelConfiguration);
    });

    final Badge boostBadge = badgeTranslator.translate(acceptableLanguages,
        oneTimeDonationConfiguration.boost().badge());
    donationLevels.put(String.valueOf(oneTimeDonationConfiguration.boost().level()),
        new LevelConfiguration(
            boostBadge.getName(),
            // NB: the one-time badges are PurchasableBadge, which has a `duration` field
            new PurchasableBadge(
                boostBadge,
                oneTimeDonationConfiguration.boost().expiration())));

    final Badge giftBadge = badgeTranslator.translate(acceptableLanguages, oneTimeDonationConfiguration.gift().badge());
    donationLevels.put(String.valueOf(oneTimeDonationConfiguration.gift().level()),
        new LevelConfiguration(
            giftBadge.getName(),
            new PurchasableBadge(
                giftBadge,
                oneTimeDonationConfiguration.gift().expiration())));

    final Map<String, BackupLevelConfiguration> backupLevels = subscriptionConfiguration.getBackupLevels()
        .entrySet().stream()
        .collect(Collectors.toMap(
            e -> String.valueOf(e.getKey()),
            ignored -> new BackupLevelConfiguration(BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES)));

    return new GetSubscriptionConfigurationResponse(buildCurrencyConfiguration(), donationLevels,
        new BackupConfiguration(backupLevels, subscriptionConfiguration.getbackupFreeTierMediaDuration().toDays()),
        oneTimeDonationConfiguration.sepaMaximumEuros());
  }

  @DELETE
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> deleteSubscriber(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.deleteSubscriber(subscriberCredentials).thenApply(unused -> Response.ok().build());
  }

  @PUT
  @Path("/{subscriberId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> updateSubscriber(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.updateSubscriber(subscriberCredentials).thenApply(record -> Response.ok().build());
  }

  record CreatePaymentMethodResponse(String clientSecret, PaymentProvider processor) {

  }

  @POST
  @Path("/{subscriberId}/create_payment_method")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPaymentMethod(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @QueryParam("type") @DefaultValue("CARD") PaymentMethod paymentMethodType,
      @HeaderParam(HttpHeaders.USER_AGENT) @Nullable final String userAgentString) throws SubscriptionException {

    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    if (paymentMethodType == PaymentMethod.PAYPAL) {
      throw new BadRequestException("The PAYPAL payment type must use create_payment_method/paypal");
    }

    final SubscriptionPaymentProcessor subscriptionPaymentProcessor = getManagerForPaymentMethod(paymentMethodType);

    return subscriptionManager.addPaymentMethodToCustomer(
            subscriberCredentials,
            subscriptionPaymentProcessor,
            getClientPlatform(userAgentString),
            SubscriptionPaymentProcessor::createPaymentMethodSetupToken)
        .thenApply(token ->
            Response.ok(new CreatePaymentMethodResponse(token, subscriptionPaymentProcessor.getProvider())).build());
  }

  public record CreatePayPalBillingAgreementRequest(@NotBlank String returnUrl, @NotBlank String cancelUrl) {}

  public record CreatePayPalBillingAgreementResponse(@NotBlank String approvalUrl, @NotBlank String token) {}

  @POST
  @Path("/{subscriberId}/create_payment_method/paypal")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPayPalPaymentMethod(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @NotNull @Valid CreatePayPalBillingAgreementRequest request,
      @Context ContainerRequestContext containerRequestContext,
      @HeaderParam(HttpHeaders.USER_AGENT) @Nullable final String userAgentString) throws SubscriptionException {

    final SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    final Locale locale = HeaderUtils.getAcceptableLanguagesForRequest(containerRequestContext).stream()
        .filter(l -> !"*".equals(l.getLanguage()))
        .findFirst()
        .orElse(Locale.US);

    return subscriptionManager.addPaymentMethodToCustomer(
            subscriberCredentials,
            braintreeManager,
            getClientPlatform(userAgentString),
            (mgr, customerId) ->
                mgr.createPayPalBillingAgreement(request.returnUrl, request.cancelUrl, locale.toLanguageTag()))
        .thenApply(billingAgreementApprovalDetails -> Response.ok(
                new CreatePayPalBillingAgreementResponse(
                    billingAgreementApprovalDetails.approvalUrl(),
                    billingAgreementApprovalDetails.billingAgreementToken()))
            .build());
  }

  private SubscriptionPaymentProcessor getManagerForPaymentMethod(PaymentMethod paymentMethod) {
    return switch (paymentMethod) {
      // Today, we always choose stripe to process non-paypal payment types, however we could use braintree to process
      // other types (like CARD) in the future.
      case CARD, SEPA_DEBIT, IDEAL -> stripeManager;
      // PAYPAL payments can only be processed with braintree
      case PAYPAL -> braintreeManager;
      case UNKNOWN -> throw new BadRequestException("Invalid payment method");
    };
  }

  private SubscriptionPaymentProcessor getManagerForProcessor(PaymentProvider processor) {
    return switch (processor) {
      case STRIPE -> stripeManager;
      case BRAINTREE -> braintreeManager;
    };
  }

  @POST
  @Path("/{subscriberId}/default_payment_method/{processor}/{paymentMethodToken}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethodWithProcessor(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("processor") PaymentProvider processor,
      @PathParam("paymentMethodToken") @NotEmpty String paymentMethodToken) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    final SubscriptionPaymentProcessor manager = getManagerForProcessor(processor);

    return setDefaultPaymentMethod(manager, paymentMethodToken, subscriberCredentials);
  }

  public record SetSubscriptionLevelSuccessResponse(long level) {
  }

  public record SetSubscriptionLevelErrorResponse(List<Error> errors) {

    public record Error(SetSubscriptionLevelErrorResponse.Error.Type type, String message) {

      public enum Type {
        UNSUPPORTED_LEVEL,
        UNSUPPORTED_CURRENCY,
        PAYMENT_REQUIRES_ACTION,
      }
    }
  }

  @PUT
  @Path("/{subscriberId}/level/{level}/{currency}/{idempotencyKey}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setSubscriptionLevel(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("level") long level,
      @PathParam("currency") String currency,
      @PathParam("idempotencyKey") String idempotencyKey) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.getSubscriber(subscriberCredentials)
        .thenCompose(record -> {
          final ProcessorCustomer processorCustomer = record.getProcessorCustomer()
              .orElseThrow(() ->
                  // a missing customer ID indicates the client made requests out of order,
                  // and needs to call create_payment_method to create a customer for the given payment method
                  new ClientErrorException(Status.CONFLICT));

          final String subscriptionTemplateId = getSubscriptionTemplateId(level, currency,
              processorCustomer.processor());

          final SubscriptionPaymentProcessor manager = getManagerForProcessor(processorCustomer.processor());
          return subscriptionManager.updateSubscriptionLevelForCustomer(subscriberCredentials, record, manager, level,
              currency, idempotencyKey, subscriptionTemplateId, this::subscriptionsAreSameType);
        })
        .exceptionally(ExceptionUtils.exceptionallyHandler(SubscriptionException.InvalidLevel.class, e -> {
          throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
              .entity(new SubscriptionController.SetSubscriptionLevelErrorResponse(List.of(
                  new SubscriptionController.SetSubscriptionLevelErrorResponse.Error(
                      SubscriptionController.SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_LEVEL,
                      null))))
              .build());
        }))
        .exceptionally(ExceptionUtils.exceptionallyHandler(SubscriptionException.PaymentRequiresAction.class, e -> {
          throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
              .entity(new SetSubscriptionLevelErrorResponse(List.of(new SetSubscriptionLevelErrorResponse.Error(
                  SetSubscriptionLevelErrorResponse.Error.Type.PAYMENT_REQUIRES_ACTION, null))))
              .build());
        }))
        .thenApply(unused -> Response.ok(new SetSubscriptionLevelSuccessResponse(level)).build());
  }

  public boolean subscriptionsAreSameType(long level1, long level2) {
    return subscriptionConfiguration.getSubscriptionLevel(level1).type()
        == subscriptionConfiguration.getSubscriptionLevel(level2).type();
  }

  @Schema(description = """
      Comprehensive configuration for donation subscriptions, backup subscriptions, gift subscriptions, and one-time
      donations pricing information for all levels are included in currencies. All levels that have an associated
      badge are included in levels.  All levels that correspond to a backup payment tier are included in
      backupLevels.""")
  public record GetSubscriptionConfigurationResponse(
      @Schema(description = "A map of lower-cased ISO 3 currency codes to minimums and level-specific scalar amounts")
      Map<String, CurrencyConfiguration> currencies,
      @Schema(description = "A map of numeric donation level IDs to level-specific badge configuration")
      Map<String, LevelConfiguration> levels,
      @Schema(description = "Backup specific configuration")
      BackupConfiguration backup,
      @Schema(description = "The maximum value of a one-time donation SEPA transaction")
      BigDecimal sepaMaximumEuros) {}

  @Schema(description = "Configuration for a currency - use to present appropriate client interfaces")
  public record CurrencyConfiguration(
      @Schema(description = "The minimum amount that may be submitted for a one-time donation in the currency")
      BigDecimal minimum,
      @Schema(description = "A map of numeric one-time donation level IDs to the list of default amounts to be presented")
      Map<String, List<BigDecimal>> oneTime,
      @Schema(description = "A map of numeric subscription level IDs to the amount charged for that level")
      Map<String, BigDecimal> subscription,
      @Schema(description = "A map of numeric backup level IDs to the amount charged for that level")
      Map<String, BigDecimal> backupSubscription,
      @Schema(description = "The payment methods that support the given currency")
      List<String> supportedPaymentMethods) {}

  @Schema(description = "Configuration for a donation level - use to present appropriate client interfaces")
  public record LevelConfiguration(
      @Schema(description = "The localized name for the level")
      String name,
      @Schema(description = "The displayable badge associated with the level")
      Badge badge) {}

  public record BackupConfiguration(
      @Schema(description = "A map of numeric backup level IDs to level-specific backup configuration")
      Map<String, BackupLevelConfiguration> levels,
      @Schema(description = "The number of days of media a free tier backup user gets")
      long backupFreeTierMediaDays) {}

  @Schema(description = "Configuration for a backup level - use to present appropriate client interfaces")
  public record BackupLevelConfiguration(
      @Schema(description = "The amount of media storage in bytes that a paying subscriber may store")
      long storageAllowanceBytes) {}

  @GET
  @Path("/configuration")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Subscription configuration ",
      description = """
          Returns all configuration for badges, donation subscriptions, backup subscriptions, and one-time donation (
          "boost" and "gift") minimum and suggested amounts.""")
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = GetSubscriptionConfigurationResponse.class)))
  public CompletableFuture<Response> getConfiguration(@Context ContainerRequestContext containerRequestContext) {
    return CompletableFuture.supplyAsync(() -> {
      List<Locale> acceptableLanguages = HeaderUtils.getAcceptableLanguagesForRequest(containerRequestContext);
      return Response.ok(buildGetSubscriptionConfigurationResponse(acceptableLanguages)).build();
    });
  }

  @GET
  @Path("/bank_mandate/{bankTransferType}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getBankMandate(final @Context ContainerRequestContext containerRequestContext,
      final @PathParam("bankTransferType") BankTransferType bankTransferType) {
    return CompletableFuture.supplyAsync(() -> {
      List<Locale> acceptableLanguages = HeaderUtils.getAcceptableLanguagesForRequest(containerRequestContext);
      return Response.ok(new GetBankMandateResponse(
          bankMandateTranslator.translate(acceptableLanguages, bankTransferType))).build();
    });
  }

  public record GetBankMandateResponse(String mandate) {}

  public record GetSubscriptionInformationResponse(
      SubscriptionController.GetSubscriptionInformationResponse.Subscription subscription,
      @JsonInclude(Include.NON_NULL) ChargeFailure chargeFailure) {

      public record Subscription(long level, Instant billingCycleAnchor, Instant endOfCurrentPeriod, boolean active,
                                 boolean cancelAtPeriodEnd, String currency, BigDecimal amount, String status,
                                 PaymentProvider processor, PaymentMethod paymentMethod, boolean paymentProcessing) {

      }
    }

  @GET
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getSubscriptionInformation(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.getSubscriber(subscriberCredentials)
        .thenCompose(record -> {
            if (record.subscriptionId == null) {
                return CompletableFuture.completedFuture(Response.ok(new GetSubscriptionInformationResponse(null, null)).build());
            }

            final SubscriptionPaymentProcessor manager = getManagerForProcessor(record.getProcessorCustomer().orElseThrow().processor());

            return manager.getSubscription(record.subscriptionId).thenCompose(subscription ->
                manager.getSubscriptionInformation(subscription).thenApply(subscriptionInformation -> Response.ok(
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
                            manager.getProvider(),
                            subscriptionInformation.paymentMethod(),
                            subscriptionInformation.paymentProcessing()),
                        subscriptionInformation.chargeFailure()
                    )).build()));
        });
  }

  public record GetReceiptCredentialsRequest(@NotEmpty byte[] receiptCredentialRequest) {
  }

  public record GetReceiptCredentialsResponse(@NotEmpty byte[] receiptCredentialResponse) {
  }

  @POST
  @Path("/{subscriberId}/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createSubscriptionReceiptCredentials(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @PathParam("subscriberId") String subscriberId,
      @NotNull @Valid GetReceiptCredentialsRequest request) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.createReceiptCredentials(subscriberCredentials, request, this::receiptExpirationWithGracePeriod)
        .thenApply(receiptCredential -> {
          final ReceiptCredentialResponse receiptCredentialResponse = receiptCredential.receiptCredentialResponse();
          final SubscriptionPaymentProcessor.ReceiptItem receipt = receiptCredential.receiptItem();
          Metrics.counter(RECEIPT_ISSUED_COUNTER_NAME,
                  Tags.of(
                      Tag.of(PROCESSOR_TAG_NAME, receiptCredential.paymentProvider().toString()),
                      Tag.of(TYPE_TAG_NAME, "subscription"),
                      Tag.of(SUBSCRIPTION_TYPE_TAG_NAME,
                          subscriptionConfiguration.getSubscriptionLevel(receipt.level()).type().name()
                              .toLowerCase(Locale.ROOT)),
                      UserAgentTagUtil.getPlatformTag(userAgent)))
              .increment();
          return Response.ok(new GetReceiptCredentialsResponse(receiptCredentialResponse.serialize())).build();
        });
  }

  @POST
  @Path("/{subscriberId}/default_payment_method_for_ideal/{setupIntentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethodForIdeal(
      @ReadOnly @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("setupIntentId") @NotEmpty String setupIntentId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    return stripeManager.getGeneratedSepaIdFromSetupIntent(setupIntentId)
        .thenCompose(generatedSepaId -> setDefaultPaymentMethod(stripeManager, generatedSepaId, subscriberCredentials));
  }

  private CompletableFuture<Response> setDefaultPaymentMethod(final SubscriptionPaymentProcessor manager,
      final String paymentMethodId,
      final SubscriberCredentials requestData) {
    return subscriptionManager.getSubscriber(requestData)
        .thenCompose(record -> record.getProcessorCustomer()
            .map(processorCustomer -> manager.setDefaultPaymentMethodForCustomer(processorCustomer.customerId(),
                paymentMethodId, record.subscriptionId))
            .orElseThrow(() ->
                // a missing customer ID indicates the client made requests out of order,
                // and needs to call create_payment_method to create a customer for the given payment method
                new ClientErrorException(Status.CONFLICT)))
        .thenApply(customer -> Response.ok().build());
  }

  private Instant receiptExpirationWithGracePeriod(SubscriptionPaymentProcessor.ReceiptItem receiptItem) {
    final Instant paidAt = receiptItem.paidAt();
    return switch (subscriptionConfiguration.getSubscriptionLevel(receiptItem.level()).type()) {
      case DONATION -> paidAt.plus(subscriptionConfiguration.getBadgeExpiration())
          .plus(subscriptionConfiguration.getBadgeGracePeriod())
          .truncatedTo(ChronoUnit.DAYS)
          .plus(1, ChronoUnit.DAYS);
      case BACKUP -> paidAt.plus(subscriptionConfiguration.getBackupExpiration())
          .truncatedTo(ChronoUnit.DAYS)
          .plus(1, ChronoUnit.DAYS);
    };
  }


  private String getSubscriptionTemplateId(long level, String currency, PaymentProvider processor) {
    final SubscriptionLevelConfiguration config = subscriptionConfiguration.getSubscriptionLevel(level);
    if (config == null) {
      throw new BadRequestException(Response.status(Status.BAD_REQUEST)
          .entity(new SetSubscriptionLevelErrorResponse(List.of(
              new SetSubscriptionLevelErrorResponse.Error(
                  SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_LEVEL, null))))
          .build());
    }
    final Optional<String> templateId = Optional
        .ofNullable(config.prices().get(currency.toLowerCase(Locale.ROOT)))
        .map(priceConfiguration -> priceConfiguration.processorIds().get(processor));
    return templateId.orElseThrow(() -> new BadRequestException(Response.status(Status.BAD_REQUEST)
        .entity(new SetSubscriptionLevelErrorResponse(List.of(
            new SetSubscriptionLevelErrorResponse.Error(
                SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_CURRENCY, null))))
        .build()));
  }

  @Nullable
  private static ClientPlatform getClientPlatform(@Nullable final String userAgentString) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgentString).getPlatform();
    } catch (final UnrecognizedUserAgentException e) {
      return null;
    }
  }
}
