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
import com.google.common.base.Strings;
import com.stripe.model.Charge;
import com.stripe.model.Charge.Outcome;
import com.stripe.model.Invoice;
import com.stripe.model.InvoiceLineItem;
import com.stripe.model.Subscription;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.math.BigDecimal;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
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
import org.whispersystems.textsecuregcm.configuration.BoostConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionPriceConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult;
import org.whispersystems.textsecuregcm.stripe.StripeManager;
import org.whispersystems.textsecuregcm.util.ExactlySize;

@Path("/v1/subscription")
public class SubscriptionController {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

  private final Clock clock;
  private final SubscriptionConfiguration subscriptionConfiguration;
  private final BoostConfiguration boostConfiguration;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;
  private final BadgeTranslator badgeTranslator;
  private final LevelTranslator levelTranslator;

  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = name(SubscriptionController.class, "invalidAcceptLanguage");

  public SubscriptionController(
      @Nonnull Clock clock,
      @Nonnull SubscriptionConfiguration subscriptionConfiguration,
      @Nonnull BoostConfiguration boostConfiguration,
      @Nonnull SubscriptionManager subscriptionManager,
      @Nonnull StripeManager stripeManager,
      @Nonnull ServerZkReceiptOperations zkReceiptOperations,
      @Nonnull IssuedReceiptsManager issuedReceiptsManager,
      @Nonnull BadgeTranslator badgeTranslator,
      @Nonnull LevelTranslator levelTranslator) {
    this.clock = Objects.requireNonNull(clock);
    this.subscriptionConfiguration = Objects.requireNonNull(subscriptionConfiguration);
    this.boostConfiguration = Objects.requireNonNull(boostConfiguration);
    this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
    this.badgeTranslator = Objects.requireNonNull(badgeTranslator);
    this.levelTranslator = Objects.requireNonNull(levelTranslator);
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
          String customerId = getResult.record.customerId;
          if (Strings.isNullOrEmpty(customerId)) {
            throw new InternalServerErrorException("no customer id found");
          }
          return stripeManager.getCustomer(customerId).thenCompose(customer -> {
            if (customer == null) {
              throw new InternalServerErrorException("no customer record found for id " + customerId);
            }
            return stripeManager.listNonCanceledSubscriptions(customer);
          }).thenCompose(subscriptions -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<Subscription>[] futures = (CompletableFuture<Subscription>[]) subscriptions.stream()
                .map(stripeManager::cancelSubscriptionAtEndOfCurrentPeriod).toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(futures);
          });
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
            return stripeManager.createCustomer(requestData.subscriberUser).thenCompose(
                customer -> subscriptionManager.create(
                        requestData.subscriberUser, requestData.hmac, customer.getId(), requestData.now)
                    .thenApply(updatedRecord -> {
                      if (updatedRecord == null) {
                        throw new NotFoundException();
                      }
                      return updatedRecord;
                    }));
          } else {
            // already exists so just touch access time and return
            return subscriptionManager.accessedAt(requestData.subscriberUser, requestData.now)
                .thenApply(unused -> getResult.record);
          }
        })
        .thenApply(record -> Response.ok().build());
  }

  public static class CreatePaymentMethodResponse {

    private final String clientSecret;

    @JsonCreator
    public CreatePaymentMethodResponse(
        @JsonProperty("clientSecret") String clientSecret) {
      this.clientSecret = clientSecret;
    }

    @SuppressWarnings("unused")
    public String getClientSecret() {
      return clientSecret;
    }
  }

  @Timed
  @POST
  @Path("/{subscriberId}/create_payment_method")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createPaymentMethod(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> stripeManager.createSetupIntent(record.customerId))
        .thenApply(setupIntent -> Response.ok(new CreatePaymentMethodResponse(setupIntent.getClientSecret())).build());
  }

  @Timed
  @POST
  @Path("/{subscriberId}/default_payment_method/{paymentMethodId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> setDefaultPaymentMethod(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      @PathParam("paymentMethodId") @NotEmpty String paymentMethodId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> stripeManager.setDefaultPaymentMethodForCustomer(record.customerId, paymentMethodId))
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
          SubscriptionLevelConfiguration levelConfiguration = subscriptionConfiguration.getLevels().get(level);
          if (levelConfiguration == null) {
            throw new BadRequestException(Response.status(Status.BAD_REQUEST)
                .entity(new SetSubscriptionLevelErrorResponse(List.of(
                    new SetSubscriptionLevelErrorResponse.Error(
                        SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_LEVEL, null))))
                .build());
          }
          SubscriptionPriceConfiguration priceConfiguration = levelConfiguration.getPrices()
              .get(currency.toLowerCase(Locale.ROOT));
          if (priceConfiguration == null) {
            throw new BadRequestException(Response.status(Status.BAD_REQUEST)
                .entity(new SetSubscriptionLevelErrorResponse(List.of(
                    new SetSubscriptionLevelErrorResponse.Error(
                        SetSubscriptionLevelErrorResponse.Error.Type.UNSUPPORTED_CURRENCY, null))))
                .build());
          }

          if (record.subscriptionId == null) {
            long lastSubscriptionCreatedAt =
                record.subscriptionCreatedAt != null ? record.subscriptionCreatedAt.getEpochSecond() : 0;
            // we don't have one yet so create it and then record the subscription id
            //
            // this relies on stripe's idempotency key to avoid creating more than one subscription if the client
            // retries this request
            return stripeManager.createSubscription(record.customerId, priceConfiguration.getId(), level,
                    lastSubscriptionCreatedAt)
                .thenCompose(subscription -> subscriptionManager.subscriptionCreated(
                        requestData.subscriberUser, subscription.getId(), requestData.now, level)
                    .thenApply(unused -> subscription));
          } else {
            // we already have a subscription in our records so let's check the level and change it if needed
            return stripeManager.getSubscription(record.subscriptionId).thenCompose(
                subscription -> stripeManager.getLevelForSubscription(subscription).thenCompose(existingLevel -> {
                  if (level == existingLevel) {
                    return CompletableFuture.completedFuture(subscription);
                  }
                  return stripeManager.updateSubscription(
                          subscription, priceConfiguration.getId(), level, idempotencyKey)
                      .thenCompose(updatedSubscription ->
                          subscriptionManager.subscriptionLevelChanged(requestData.subscriberUser, requestData.now,
                                  level)
                              .thenApply(unused -> updatedSubscription));
                }));
          }
        })
        .thenApply(subscription -> Response.ok(new SetSubscriptionLevelSuccessResponse(level)).build());
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

  @Timed
  @GET
  @Path("/levels")
  @Produces(MediaType.APPLICATION_JSON)
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
                          levelEntry -> levelEntry.getValue().getAmount()))))));
      return Response.ok(getLevelsResponse).build();
    });
  }

  public static class GetBoostBadgesResponse {
    public static class Level {
      private final Badge badge;

      @JsonCreator
      public Level(
          @JsonProperty("badge") Badge badge) {
        this.badge = badge;
      }

      public Badge getBadge() {
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
  public CompletableFuture<Response> getBoostBadges(@Context ContainerRequestContext containerRequestContext) {
    return CompletableFuture.supplyAsync(() -> {
      long boostLevel = boostConfiguration.getLevel();
      String boostBadge = boostConfiguration.getBadge();
      List<Locale> acceptableLanguages = getAcceptableLanguagesForRequest(containerRequestContext);
      GetBoostBadgesResponse getBoostBadgesResponse = new GetBoostBadgesResponse(Map.of(boostLevel,
          new GetBoostBadgesResponse.Level(badgeTranslator.translate(acceptableLanguages, boostBadge))));
      return Response.ok(getBoostBadgesResponse).build();
    });
  }

  @Timed
  @GET
  @Path("/boost/amounts")
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getBoostAmounts() {
    return CompletableFuture.supplyAsync(() -> Response.ok(
        boostConfiguration.getCurrencies().entrySet().stream().collect(
            Collectors.toMap(entry -> entry.getKey().toUpperCase(Locale.ROOT), Entry::getValue))).build());
  }

  public static class CreateBoostRequest {

    private final String currency;
    private final long amount;

    @JsonCreator
    public CreateBoostRequest(
        @JsonProperty("currency") String currency,
        @JsonProperty("amount") long amount) {
      this.currency = currency;
      this.amount = amount;
    }

    @NotEmpty
    @ExactlySize(3)
    public String getCurrency() {
      return currency;
    }

    @Min(1)
    public long getAmount() {
      return amount;
    }
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

  @Timed
  @POST
  @Path("/boost/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> createBoostPaymentIntent(@NotNull @Valid CreateBoostRequest request) {
    return stripeManager.createPaymentIntent(request.getCurrency(), request.getAmount())
        .thenApply(paymentIntent -> Response.ok(new CreateBoostResponse(paymentIntent.getClientSecret())).build());
  }

  public static class CreateBoostReceiptCredentialsRequest {

    private final String paymentIntentId;
    private final byte[] receiptCredentialRequest;

    @JsonCreator
    public CreateBoostReceiptCredentialsRequest(
        @JsonProperty("paymentIntentId") String paymentIntentId,
        @JsonProperty("receiptCredentialRequest") byte[] receiptCredentialRequest) {
      this.paymentIntentId = paymentIntentId;
      this.receiptCredentialRequest = receiptCredentialRequest;
    }

    @NotNull
    public String getPaymentIntentId() {
      return paymentIntentId;
    }

    @NotNull
    public byte[] getReceiptCredentialRequest() {
      return receiptCredentialRequest;
    }
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
    return stripeManager.getPaymentIntent(request.getPaymentIntentId())
        .thenCompose(paymentIntent -> {
          if (paymentIntent == null) {
            throw new WebApplicationException(Status.NOT_FOUND);
          }
          if (StringUtils.equalsIgnoreCase("processing", paymentIntent.getStatus())) {
            throw new WebApplicationException(Status.NO_CONTENT);
          }
          if (!StringUtils.equalsIgnoreCase("succeeded", paymentIntent.getStatus())) {
            throw new WebApplicationException(Status.PAYMENT_REQUIRED);
          }
          ReceiptCredentialRequest receiptCredentialRequest;
          try {
            receiptCredentialRequest = new ReceiptCredentialRequest(request.getReceiptCredentialRequest());
          } catch (InvalidInputException e) {
            throw new BadRequestException("invalid receipt credential request", e);
          }
          return issuedReceiptsManager.recordIssuance(paymentIntent.getId(), receiptCredentialRequest, clock.instant())
              .thenApply(unused -> {
                Instant expiration = Instant.ofEpochSecond(paymentIntent.getCreated())
                    .plus(boostConfiguration.getExpiration())
                    .truncatedTo(ChronoUnit.DAYS)
                    .plus(1, ChronoUnit.DAYS);
                ReceiptCredentialResponse receiptCredentialResponse;
                try {
                  receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                      receiptCredentialRequest, expiration.getEpochSecond(), boostConfiguration.getLevel());
                } catch (VerificationFailedException e) {
                  throw new BadRequestException("receipt credential request failed verification", e);
                }
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

      @JsonCreator
      public Subscription(
          @JsonProperty("level") long level,
          @JsonProperty("billingCycleAnchor") Instant billingCycleAnchor,
          @JsonProperty("endOfCurrentPeriod") Instant endOfCurrentPeriod,
          @JsonProperty("active") boolean active,
          @JsonProperty("cancelAtPeriodEnd") boolean cancelAtPeriodEnd,
          @JsonProperty("currency") String currency,
          @JsonProperty("amount") BigDecimal amount,
          @JsonProperty("status") String status) {
        this.level = level;
        this.billingCycleAnchor = billingCycleAnchor;
        this.endOfCurrentPeriod = endOfCurrentPeriod;
        this.active = active;
        this.cancelAtPeriodEnd = cancelAtPeriodEnd;
        this.currency = currency;
        this.amount = amount;
        this.status = status;
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
          return stripeManager.getSubscription(record.subscriptionId).thenCompose(subscription ->
              stripeManager.getPriceForSubscription(subscription).thenCompose(price ->
                  stripeManager.getLevelForPrice(price).thenApply(level -> {
                    GetSubscriptionInformationResponse.ChargeFailure chargeFailure = null;
                    if (subscription.getLatestInvoiceObject() != null && subscription.getLatestInvoiceObject().getChargeObject() != null &&
                        (subscription.getLatestInvoiceObject().getChargeObject().getFailureCode() != null || subscription.getLatestInvoiceObject().getChargeObject().getFailureMessage() != null)) {
                      Charge charge = subscription.getLatestInvoiceObject().getChargeObject();
                      Outcome outcome = charge.getOutcome();
                      chargeFailure = new GetSubscriptionInformationResponse.ChargeFailure(
                          charge.getFailureCode(),
                          charge.getFailureMessage(),
                          outcome != null ? outcome.getNetworkStatus() : null,
                          outcome != null ? outcome.getReason() : null,
                          outcome != null ? outcome.getType() : null);
                    }
                    return Response.ok(
                        new GetSubscriptionInformationResponse(
                            new GetSubscriptionInformationResponse.Subscription(
                                level,
                                Instant.ofEpochSecond(subscription.getBillingCycleAnchor()),
                                Instant.ofEpochSecond(subscription.getCurrentPeriodEnd()),
                                Objects.equals(subscription.getStatus(), "active"),
                                subscription.getCancelAtPeriodEnd(),
                                price.getCurrency().toUpperCase(Locale.ROOT),
                                price.getUnitAmountDecimal(),
                                subscription.getStatus()),
                            chargeFailure
                        )).build();
                  })));
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
          return stripeManager.getLatestInvoiceForSubscription(record.subscriptionId)
              .thenCompose(invoice -> convertInvoiceToReceipt(invoice, record.subscriptionId))
              .thenCompose(receipt -> issuedReceiptsManager.recordIssuance(
                      receipt.getInvoiceLineItemId(), receiptCredentialRequest, requestData.now)
                  .thenApply(unused -> receipt))
              .thenApply(receipt -> {
                ReceiptCredentialResponse receiptCredentialResponse;
                try {
                  receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                      receiptCredentialRequest, receipt.getExpiration().getEpochSecond(), receipt.getLevel());
                } catch (VerificationFailedException e) {
                  throw new BadRequestException("receipt credential request failed verification", e);
                }
                return Response.ok(new GetReceiptCredentialsResponse(receiptCredentialResponse.serialize())).build();
              });
        });
  }

  public static class Receipt {

    private final Instant expiration;
    private final long level;
    private final String invoiceLineItemId;

    public Receipt(Instant expiration, long level, String invoiceLineItemId) {
      this.expiration = expiration;
      this.level = level;
      this.invoiceLineItemId = invoiceLineItemId;
    }

    public Instant getExpiration() {
      return expiration;
    }

    public long getLevel() {
      return level;
    }

    public String getInvoiceLineItemId() {
      return invoiceLineItemId;
    }
  }

  private CompletableFuture<Receipt> convertInvoiceToReceipt(Invoice latestSubscriptionInvoice, String subscriptionId) {
    if (latestSubscriptionInvoice == null) {
      throw new WebApplicationException(Status.NO_CONTENT);
    }
    if (StringUtils.equalsIgnoreCase("open", latestSubscriptionInvoice.getStatus())) {
      throw new WebApplicationException(Status.NO_CONTENT);
    }
    if (!StringUtils.equalsIgnoreCase("paid", latestSubscriptionInvoice.getStatus())) {
      throw new WebApplicationException(Status.PAYMENT_REQUIRED);
    }

    return stripeManager.getInvoiceLineItemsForInvoice(latestSubscriptionInvoice).thenCompose(invoiceLineItems -> {
      Collection<InvoiceLineItem> subscriptionLineItems = invoiceLineItems.stream()
          .filter(invoiceLineItem -> Objects.equals("subscription", invoiceLineItem.getType()))
          .collect(Collectors.toList());
      if (subscriptionLineItems.isEmpty()) {
        throw new IllegalStateException("latest subscription invoice has no subscription line items; subscriptionId="
            + subscriptionId + "; invoiceId=" + latestSubscriptionInvoice.getId());
      }
      if (subscriptionLineItems.size() > 1) {
        throw new IllegalStateException(
            "latest subscription invoice has too many subscription line items; subscriptionId=" + subscriptionId
                + "; invoiceId=" + latestSubscriptionInvoice.getId() + "; count=" + subscriptionLineItems.size());
      }

      InvoiceLineItem subscriptionLineItem = subscriptionLineItems.stream().findAny().get();
      return getReceiptForSubscriptionInvoiceLineItem(subscriptionLineItem);
    });
  }

  private CompletableFuture<Receipt> getReceiptForSubscriptionInvoiceLineItem(InvoiceLineItem subscriptionLineItem) {
    return stripeManager.getProductForPrice(subscriptionLineItem.getPrice().getId()).thenApply(product -> new Receipt(
        Instant.ofEpochSecond(subscriptionLineItem.getPeriod().getEnd())
            .plus(subscriptionConfiguration.getBadgeGracePeriod())
            .truncatedTo(ChronoUnit.DAYS)
            .plus(1, ChronoUnit.DAYS),
        stripeManager.getLevelForProduct(product),
        subscriptionLineItem.getId()));
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
