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
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
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
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationCurrencyConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriberCredentials;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BankTransferType;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

@Path("/v1/subscription")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Subscriptions")
public class SubscriptionController {

  private final Clock clock;
  private final SubscriptionConfiguration subscriptionConfiguration;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final BraintreeManager braintreeManager;
  private final GooglePlayBillingManager googlePlayBillingManager;
  private final AppleAppStoreManager appleAppStoreManager;
  private final BadgeTranslator badgeTranslator;
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
      @Nonnull GooglePlayBillingManager googlePlayBillingManager,
      @Nonnull AppleAppStoreManager appleAppStoreManager,
      @Nonnull BadgeTranslator badgeTranslator,
      @Nonnull BankMandateTranslator bankMandateTranslator) {
    this.subscriptionManager = subscriptionManager;
    this.clock = Objects.requireNonNull(clock);
    this.subscriptionConfiguration = Objects.requireNonNull(subscriptionConfiguration);
    this.oneTimeDonationConfiguration = Objects.requireNonNull(oneTimeDonationConfiguration);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.braintreeManager = Objects.requireNonNull(braintreeManager);
    this.googlePlayBillingManager = Objects.requireNonNull(googlePlayBillingManager);
    this.appleAppStoreManager = appleAppStoreManager;
    this.badgeTranslator = Objects.requireNonNull(badgeTranslator);
    this.bankMandateTranslator = Objects.requireNonNull(bankMandateTranslator);
  }

  private Map<String, CurrencyConfiguration> buildCurrencyConfiguration() {
    final List<CustomerAwareSubscriptionPaymentProcessor> subscriptionPaymentProcessors = List.of(stripeManager, braintreeManager);
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
          "" /* deprecated and unused */,
          badgeTranslator.translate(acceptableLanguages, levelConfig.badge()));
      donationLevels.put(String.valueOf(levelId), levelConfiguration);
    });

    final Badge boostBadge = badgeTranslator.translate(acceptableLanguages,
        oneTimeDonationConfiguration.boost().badge());
    donationLevels.put(String.valueOf(oneTimeDonationConfiguration.boost().level()),
        new LevelConfiguration(
            "" /* deprecated and unused */,
            // NB: the one-time badges are PurchasableBadge, which has a `duration` field
            new PurchasableBadge(
                boostBadge,
                oneTimeDonationConfiguration.boost().expiration())));

    final Badge giftBadge = badgeTranslator.translate(acceptableLanguages, oneTimeDonationConfiguration.gift().badge());
    donationLevels.put(String.valueOf(oneTimeDonationConfiguration.gift().level()),
        new LevelConfiguration(
            "" /* deprecated and unused */,
            new PurchasableBadge(
                giftBadge,
                oneTimeDonationConfiguration.gift().expiration())));

    final Map<String, BackupLevelConfiguration> backupLevels = subscriptionConfiguration.getBackupLevels()
        .entrySet().stream()
        .collect(Collectors.toMap(
            e -> String.valueOf(e.getKey()),
            e -> new BackupLevelConfiguration(
                BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES,
                e.getValue().playProductId(),
                e.getValue().mediaTtl().toDays())));

    return new GetSubscriptionConfigurationResponse(buildCurrencyConfiguration(), donationLevels,
        new BackupConfiguration(backupLevels, subscriptionConfiguration.getbackupFreeTierMediaDuration().toDays()),
        oneTimeDonationConfiguration.sepaMaximumEuros());
  }

  @DELETE
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> deleteSubscriber(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
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
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
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
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @QueryParam("type") @DefaultValue("CARD") PaymentMethod paymentMethodType,
      @HeaderParam(HttpHeaders.USER_AGENT) @Nullable final String userAgentString) throws SubscriptionException {

    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    final CustomerAwareSubscriptionPaymentProcessor customerAwareSubscriptionPaymentProcessor = switch (paymentMethodType) {
      // Today, we always choose stripe to process non-paypal payment types, however we could use braintree to process
      // other types (like CARD) in the future.
      case CARD, SEPA_DEBIT, IDEAL -> stripeManager;
      case GOOGLE_PLAY_BILLING, APPLE_APP_STORE ->
          throw new BadRequestException("cannot create payment methods with payment type " + paymentMethodType);
      case PAYPAL -> throw new BadRequestException("The PAYPAL payment type must use create_payment_method/paypal");
      case UNKNOWN -> throw new BadRequestException("Invalid payment method");
    };

    return subscriptionManager.addPaymentMethodToCustomer(
            subscriberCredentials,
            customerAwareSubscriptionPaymentProcessor,
            getClientPlatform(userAgentString),
            CustomerAwareSubscriptionPaymentProcessor::createPaymentMethodSetupToken)
        .thenApply(token ->
            Response.ok(new CreatePaymentMethodResponse(token, customerAwareSubscriptionPaymentProcessor.getProvider())).build());
  }

  public record CreatePayPalBillingAgreementRequest(@NotBlank String returnUrl, @NotBlank String cancelUrl) {}

  public record CreatePayPalBillingAgreementResponse(@NotBlank String approvalUrl, @NotBlank String token) {}

  @POST
  @Path("/{subscriberId}/create_payment_method/paypal")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPayPalPaymentMethod(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
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

  private CustomerAwareSubscriptionPaymentProcessor getCustomerAwareProcessor(PaymentProvider processor) {
    return switch (processor) {
      case STRIPE -> stripeManager;
      case BRAINTREE -> braintreeManager;
      case GOOGLE_PLAY_BILLING, APPLE_APP_STORE -> throw new BadRequestException("Operation cannot be performed with the " + processor + " payment provider");
    };
  }

  @POST
  @Path("/{subscriberId}/default_payment_method/{processor}/{paymentMethodToken}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethodWithProcessor(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("processor") PaymentProvider processor,
      @PathParam("paymentMethodToken") @NotEmpty String paymentMethodToken) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    final CustomerAwareSubscriptionPaymentProcessor manager = getCustomerAwareProcessor(processor);

    return setDefaultPaymentMethod(manager, paymentMethodToken, subscriberCredentials);
  }

  public record SetSubscriptionLevelSuccessResponse(long level) {
  }

  public record SetSubscriptionLevelErrorResponse(List<Error> errors) {

    public record Error(SetSubscriptionLevelErrorResponse.Error.Type type, String message) {

      public enum Type {
        // The requested level was invalid
        UNSUPPORTED_LEVEL,
        // The requested currency was invalid
        UNSUPPORTED_CURRENCY,
        // The card could not be charged
        PAYMENT_REQUIRES_ACTION,
        // The request arguments were invalid representing a programmer error
        INVALID_ARGUMENTS
      }
    }
  }

  @PUT
  @Path("/{subscriberId}/level/{level}/{currency}/{idempotencyKey}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setSubscriptionLevel(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
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

          final CustomerAwareSubscriptionPaymentProcessor manager = getCustomerAwareProcessor(processorCustomer.processor());
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
        .exceptionally(ExceptionUtils.exceptionallyHandler(SubscriptionException.InvalidArguments.class, e -> {
          throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST)
              .entity(new SetSubscriptionLevelErrorResponse(List.of(new SetSubscriptionLevelErrorResponse.Error(
                  SetSubscriptionLevelErrorResponse.Error.Type.INVALID_ARGUMENTS, e.getMessage()))))
              .build());
        }))
        .thenApply(unused -> Response.ok(new SetSubscriptionLevelSuccessResponse(level)).build());
  }

  public boolean subscriptionsAreSameType(long level1, long level2) {
    return subscriptionConfiguration.getSubscriptionLevel(level1).type()
        == subscriptionConfiguration.getSubscriptionLevel(level2).type();
  }

  @POST
  @Path("/{subscriberId}/appstore/{originalTransactionId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Set app store subscription", description = """
  Set an originalTransactionId that represents an IAP subscription made with the app store.
  
  To set up an app store subscription:
  1. Create a subscriber with `PUT subscriptions/{subscriberId}` (you must regularly refresh this subscriber)
  2. [Create a subscription](https://developer.apple.com/documentation/storekit/in-app_purchase/) with the App Store
     directly via StoreKit and obtain a originalTransactionId.
  3. `POST` the purchaseToken here
  4. Obtain a receipt at `POST /v1/subscription/{subscriberId}/receipt_credentials` which can then be used to obtain the
     entitlement
  """)
  @ApiResponse(responseCode = "200", description = "The originalTransactionId was successfully validated")
  @ApiResponse(responseCode = "402", description = "The subscription transaction is incomplete or invalid")
  @ApiResponse(responseCode = "403", description = "subscriberId authentication failure OR account authentication is present")
  @ApiResponse(responseCode = "404", description = "No such subscriberId exists or subscriberId is malformed or the specified transaction does not exist")
  @ApiResponse(responseCode = "409", description = "subscriberId is already linked to a processor that does not support appstore payments. Delete this subscriberId and use a new one.")
  @ApiResponse(responseCode = "429", description = "Rate limit exceeded.")
  public CompletableFuture<SetSubscriptionLevelSuccessResponse> setAppStoreSubscription(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("originalTransactionId") String originalTransactionId) throws SubscriptionException {
    final SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    return subscriptionManager
        .updateAppStoreTransactionId(subscriberCredentials, appleAppStoreManager, originalTransactionId)
        .thenApply(SetSubscriptionLevelSuccessResponse::new);
  }


  @POST
  @Path("/{subscriberId}/playbilling/{purchaseToken}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Set a google play billing purchase token", description = """
  Set a purchaseToken that represents an IAP subscription made with Google Play Billing.

  To set up a subscription with Google Play Billing:
  1. Create a subscriber with `PUT subscriptions/{subscriberId}` (you must regularly refresh this subscriber)
  2. [Create a subscription](https://developer.android.com/google/play/billing/integrate) with Google Play Billing
     directly and obtain a purchaseToken. Do not [acknowledge](https://developer.android.com/google/play/billing/integrate#subscriptions)
     the purchaseToken.
  3. `POST` the purchaseToken here
  4. Obtain a receipt at `POST /v1/subscription/{subscriberId}/receipt_credentials` which can then be used to obtain the
     entitlement

  After calling this method, the payment is confirmed. Callers must durably store their subscriberId before calling
  this method to ensure their payment is tracked.

  Once a purchaseToken to is posted to a subscriberId, the same subscriberId must not be used with another payment
  method. A different playbilling purchaseToken can be posted to the same subscriberId, in this case the subscription
  associated with the old purchaseToken will be cancelled.
  """)
  @ApiResponse(responseCode = "200", description = "The purchaseToken was validated and acknowledged")
  @ApiResponse(responseCode = "402", description = "The purchaseToken payment is incomplete or invalid")
  @ApiResponse(responseCode = "403", description = "subscriberId authentication failure OR account authentication is present")
  @ApiResponse(responseCode = "404", description = "No such subscriberId exists or subscriberId is malformed or the purchaseToken does not exist")
  @ApiResponse(responseCode = "409", description = "subscriberId is already linked to a processor that does not support Play Billing. Delete this subscriberId and use a new one.")
  public CompletableFuture<SetSubscriptionLevelSuccessResponse> setPlayStoreSubscription(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("purchaseToken") String purchaseToken) throws SubscriptionException {
    final SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    return subscriptionManager
        .updatePlayBillingPurchaseToken(subscriberCredentials, googlePlayBillingManager, purchaseToken)
        .thenApply(SetSubscriptionLevelSuccessResponse::new);
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
      @Deprecated(forRemoval = true) // may be removed after 2025-01-28
      @Schema(description = "The localized name for the level")
      String name,
      @Schema(description = "The displayable badge associated with the level")
      Badge badge) {}

  public record BackupConfiguration(
      @Schema(description = "A map of numeric backup level IDs to level-specific backup configuration")
      Map<String, BackupLevelConfiguration> levels,
      @Schema(description = "The number of days of media a free tier backup user gets")
      long freeTierMediaDays) {}

  @Schema(description = "Configuration for a backup level - use to present appropriate client interfaces")
  public record BackupLevelConfiguration(
      @Schema(description = "The amount of media storage in bytes that a paying subscriber may store")
      long storageAllowanceBytes,
      @Schema(description = "The play billing productID associated with this backup level")
      String playProductId,
      @Schema(description = "The duration, in days, for which your backed up media is retained on the server after you stop refreshing with a paid credential")
      long mediaTtlDays) {}

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
      @Schema(description = "Information about the subscription, or null if no subscription is present")
      SubscriptionController.GetSubscriptionInformationResponse.Subscription subscription,
      @Schema(description = "May be omitted entirely if no charge failure is detected")
      @JsonInclude(Include.NON_NULL) ChargeFailure chargeFailure) {

    public record Subscription(
        @Schema(description = "The subscription level")
        long level,

        @Schema(
            description = "If present, UNIX Epoch Timestamp in seconds, can be used to calculate next billing date.",
            externalDocs = @ExternalDocumentation(description = "Calculate next billing date", url = "https://stripe.com/docs/billing/subscriptions/billing-cycle"))
        Instant billingCycleAnchor,

        @Schema(description = "UNIX Epoch Timestamp in seconds, when the current subscription period ends")
        Instant endOfCurrentPeriod,

        @Schema(description = "Whether there is a currently active subscription")
        boolean active,

        @Schema(description = "If true, an active subscription will not auto-renew at the end of the current period")
        boolean cancelAtPeriodEnd,

        @Schema(description = "A three-letter ISO 4217 currency code for currency used in the subscription")
        String currency,

        @Schema(
            description = "The amount paid for the subscription in the currency's smallest unit",
            externalDocs = @ExternalDocumentation(description = "Stripe Currencies", url = "https://docs.stripe.com/currencies"))
        BigDecimal amount,

        @Schema(
            description = "The subscription's status, mapped to Stripe's statuses. trialing will never be returned",
            externalDocs = @ExternalDocumentation(description = "Stripe subscription statuses", url = "https://docs.stripe.com/billing/subscriptions/overview#subscription-statuses"))
        String status,

        @Schema(description = "The payment provider associated with the subscription")
        PaymentProvider processor,

        @Schema(description = "The payment method associated with the subscription")
        PaymentMethod paymentMethod,

        @Schema(description = "Whether the latest invoice for the subscription is in a non-terminal state")
        boolean paymentProcessing) {}
  }

  @GET
  @Path("/{subscriberId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Subscription information", description = """
      Returns information about the current subscription associated with the provided subscriberId if one exists.
  
      Although it uses [Stripe’s values](https://stripe.com/docs/billing/subscriptions/overview#subscription-statuses),
      the status field in the response is generic, with [Braintree-specific values](https://developer.paypal.com/braintree/docs/guides/recurring-billing/overview#subscription-statuses) mapped
      to Stripe's. Since we don’t support trials or unpaid subscriptions, the associated statuses will never be returned
      by the API.
      """)
  @ApiResponse(responseCode = "200", description = "The subscriberId exists", content = @Content(schema = @Schema(implementation = GetSubscriptionInformationResponse.class)))
  @ApiResponse(responseCode = "403", description = "subscriberId authentication failure OR account authentication is present")
  @ApiResponse(responseCode = "404", description = "No such subscriberId exists or subscriberId is malformed")
  public CompletableFuture<Response> getSubscriptionInformation(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.getSubscriptionInformation(subscriberCredentials).thenApply(maybeInfo -> maybeInfo
        .map(subscriptionInformation -> Response.ok(
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
                    subscriptionInformation.paymentProvider(),
                    subscriptionInformation.paymentMethod(),
                    subscriptionInformation.paymentProcessing()),
                subscriptionInformation.chargeFailure()
            )).build())
        .orElseGet(() -> Response.ok(new GetSubscriptionInformationResponse(null, null)).build()));
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
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @PathParam("subscriberId") String subscriberId,
      @NotNull @Valid GetReceiptCredentialsRequest request) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials = SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.createReceiptCredentials(subscriberCredentials, request, this::receiptExpirationWithGracePeriod)
        .thenApply(receiptCredential -> {
          final ReceiptCredentialResponse receiptCredentialResponse = receiptCredential.receiptCredentialResponse();
          final CustomerAwareSubscriptionPaymentProcessor.ReceiptItem receipt = receiptCredential.receiptItem();
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
        })
        .exceptionally(ExceptionUtils.exceptionallyHandler(
            SubscriptionException.ReceiptRequestedForOpenPayment.class,
            e -> Response.noContent().build()));
  }

  @POST
  @Path("/{subscriberId}/default_payment_method_for_ideal/{setupIntentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethodForIdeal(
      @Auth Optional<AuthenticatedDevice> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("setupIntentId") @NotEmpty String setupIntentId) throws SubscriptionException {
    SubscriberCredentials subscriberCredentials =
        SubscriberCredentials.process(authenticatedAccount, subscriberId, clock);

    return stripeManager.getGeneratedSepaIdFromSetupIntent(setupIntentId)
        .thenCompose(generatedSepaId -> setDefaultPaymentMethod(stripeManager, generatedSepaId, subscriberCredentials));
  }

  private CompletableFuture<Response> setDefaultPaymentMethod(final CustomerAwareSubscriptionPaymentProcessor manager,
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
        .exceptionally(ExceptionUtils.exceptionallyHandler(SubscriptionException.InvalidArguments.class, e -> {
          // Here, invalid arguments must mean that the client has made requests out of order, and needs to finish
          // setting up the paymentMethod first
          throw new ClientErrorException(Status.CONFLICT);
        }))
        .thenApply(customer -> Response.ok().build());
  }

  private Instant receiptExpirationWithGracePeriod(CustomerAwareSubscriptionPaymentProcessor.ReceiptItem receiptItem) {
    final PaymentTime paymentTime = receiptItem.paymentTime();
    return switch (subscriptionConfiguration.getSubscriptionLevel(receiptItem.level()).type()) {
      case DONATION -> paymentTime.receiptExpiration(
          subscriptionConfiguration.getBadgeExpiration(),
          subscriptionConfiguration.getBadgeGracePeriod());
      case BACKUP -> paymentTime.receiptExpiration(
          subscriptionConfiguration.getBackupExpiration(),
          subscriptionConfiguration.getBackupGracePeriod());
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
      return UserAgentUtil.parseUserAgentString(userAgentString).platform();
    } catch (final UnrecognizedUserAgentException e) {
      return null;
    }
  }
}
