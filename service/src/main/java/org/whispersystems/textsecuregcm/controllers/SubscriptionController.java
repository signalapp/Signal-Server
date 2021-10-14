/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.stripe.model.Invoice;
import com.stripe.model.InvoiceLineItem;
import com.stripe.model.Subscription;
import io.dropwizard.auth.Auth;
import java.math.BigDecimal;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
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
import javax.validation.constraints.NotEmpty;
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
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.VerificationFailedException;
import org.signal.zkgroup.receipts.ReceiptCredentialRequest;
import org.signal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionPriceConfiguration;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult;
import org.whispersystems.textsecuregcm.stripe.StripeManager;
import org.whispersystems.textsecuregcm.util.ExactlySize;

@Path("/v1/subscription")
public class SubscriptionController {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

  private final Clock clock;
  private final SubscriptionConfiguration config;
  private final SubscriptionManager subscriptionManager;
  private final StripeManager stripeManager;
  private final ServerZkReceiptOperations zkReceiptOperations;
  private final IssuedReceiptsManager issuedReceiptsManager;

  public SubscriptionController(
      @Nonnull Clock clock,
      @Nonnull SubscriptionConfiguration config,
      @Nonnull SubscriptionManager subscriptionManager,
      @Nonnull StripeManager stripeManager,
      @Nonnull ServerZkReceiptOperations zkReceiptOperations,
      @Nonnull IssuedReceiptsManager issuedReceiptsManager) {
    this.clock = Objects.requireNonNull(clock);
    this.config = Objects.requireNonNull(config);
    this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
    this.stripeManager = Objects.requireNonNull(stripeManager);
    this.zkReceiptOperations = Objects.requireNonNull(zkReceiptOperations);
    this.issuedReceiptsManager = Objects.requireNonNull(issuedReceiptsManager);
  }

  @Timed
  @DELETE
  @Path("/{subscriberId}")
  @Consumes(MediaType.APPLICATION_JSON)
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
          SubscriptionLevelConfiguration levelConfiguration = config.getLevels().get(level);
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
            // we don't have one yet so create it and then record the subscription id
            //
            // this relies on stripe's idempotency key to avoid creating more than one subscription if the client
            // retries this request
            return stripeManager.createSubscription(record.customerId, priceConfiguration.getId(), level)
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

      private final String badgeId;
      private final Map<String, BigDecimal> currencies;

      @JsonCreator
      public Level(
          @JsonProperty("badgeId") String badgeId,
          @JsonProperty("currencies") Map<String, BigDecimal> currencies) {
        this.badgeId = badgeId;
        this.currencies = currencies;
      }

      public String getBadgeId() {
        return badgeId;
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
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getLevels() {
    return CompletableFuture.supplyAsync(() -> {
      GetLevelsResponse getLevelsResponse = new GetLevelsResponse(
          config.getLevels().entrySet().stream().collect(Collectors.toMap(Entry::getKey,
              entry -> new GetLevelsResponse.Level(entry.getValue().getBadge(),
                  entry.getValue().getPrices().entrySet().stream().collect(
                      Collectors.toMap(levelEntry -> levelEntry.getKey().toUpperCase(Locale.ROOT),
                          levelEntry -> levelEntry.getValue().getAmount()))))));
      return Response.ok(getLevelsResponse).build();
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

      public Subscription(
          @JsonProperty("level") long level,
          @JsonProperty("billingCycleAnchor") Instant billingCycleAnchor,
          @JsonProperty("endOfCurrentPeriod") Instant endOfCurrentPeriod,
          @JsonProperty("active") boolean active,
          @JsonProperty("cancelAtPeriodEnd") boolean cancelAtPeriodEnd,
          @JsonProperty("currency") String currency,
          @JsonProperty("amount") BigDecimal amount) {
        this.level = level;
        this.billingCycleAnchor = billingCycleAnchor;
        this.endOfCurrentPeriod = endOfCurrentPeriod;
        this.active = active;
        this.cancelAtPeriodEnd = cancelAtPeriodEnd;
        this.currency = currency;
        this.amount = amount;
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
    }

    private final Subscription subscription;

    @JsonCreator
    public GetSubscriptionInformationResponse(
        @JsonProperty("subscription") Subscription subscription) {
      this.subscription = subscription;
    }

    public Subscription getSubscription() {
      return subscription;
    }
  }

  @Timed
  @GET
  @Path("/{subscriberId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getSubscriptionInformation(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {
          if (record.subscriptionId == null) {
            return CompletableFuture.completedFuture(Response.ok(new GetSubscriptionInformationResponse(null)).build());
          }
          return stripeManager.getSubscription(record.subscriptionId).thenCompose(subscription ->
              stripeManager.getPriceForSubscription(subscription).thenCompose(price ->
                  stripeManager.getLevelForPrice(price).thenApply(level -> Response.ok(
                      new GetSubscriptionInformationResponse(new GetSubscriptionInformationResponse.Subscription(
                          level,
                          Instant.ofEpochSecond(subscription.getBillingCycleAnchor()),
                          Instant.ofEpochSecond(subscription.getCurrentPeriodEnd()),
                          Objects.equals(subscription.getStatus(), "active"),
                          subscription.getCancelAtPeriodEnd(),
                          price.getCurrency().toUpperCase(Locale.ROOT),
                          price.getUnitAmountDecimal()
                      ))).build())));
        });
  }

  public static class GetReceiptCredentialsRequest {

    private final byte[] receiptCredentialRequest;

    @JsonCreator
    public GetReceiptCredentialsRequest(
        @JsonProperty("receiptCredentialRequest") byte[] receiptCredentialRequest) {
      this.receiptCredentialRequest = receiptCredentialRequest;
    }

    @ExactlySize(ReceiptCredentialRequest.SIZE)
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

    @ExactlySize(ReceiptCredentialResponse.SIZE)
    public byte[] getReceiptCredentialResponse() {
      return receiptCredentialResponse;
    }
  }

  @Timed
  @POST
  @Path("/{subscriberId}/receipt_credentials")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<Response> getReceiptCredentials(
      @Auth Optional<AuthenticatedAccount> authenticatedAccount,
      @PathParam("subscriberId") String subscriberId,
      GetReceiptCredentialsRequest request) {
    RequestData requestData = RequestData.process(authenticatedAccount, subscriberId, clock);
    return subscriptionManager.get(requestData.subscriberUser, requestData.hmac)
        .thenApply(this::requireRecordFromGetResult)
        .thenCompose(record -> {
          if (record.subscriptionId == null) {
            return CompletableFuture.completedFuture(Response.noContent().build());
          }
          ReceiptCredentialRequest receiptCredentialRequest;
          try {
            receiptCredentialRequest = new ReceiptCredentialRequest(request.getReceiptCredentialRequest());
          } catch (InvalidInputException e) {
            throw new BadRequestException("invalid receipt credential request", e);
          }
          return stripeManager.getPaidInvoicesForSubscription(record.subscriptionId, requestData.now)
              .thenCompose(invoices -> checkNextInvoice(invoices.iterator(), record.subscriptionId))
              .thenCompose(receipt -> {
                if (receipt == null) {
                  return CompletableFuture.completedFuture(null);
                }
                return issuedReceiptsManager.recordIssuance(
                    receipt.invoiceLineItemId, receiptCredentialRequest, requestData.now).thenApply(unused -> receipt);
              })
              .thenApply(receipt -> {
                if (receipt == null) {
                  return Response.noContent().build();
                } else {
                  ReceiptCredentialResponse receiptCredentialResponse;
                  try {
                    receiptCredentialResponse = zkReceiptOperations.issueReceiptCredential(
                        receiptCredentialRequest, receipt.getExpiration().getEpochSecond(), receipt.getLevel());
                  } catch (VerificationFailedException e) {
                    throw new BadRequestException("receipt credential request failed verification", e);
                  }
                  return Response.ok(new GetReceiptCredentialsResponse(receiptCredentialResponse.serialize())).build();
                }
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

  private CompletableFuture<Receipt> checkNextInvoice(Iterator<Invoice> invoiceIterator, String subscriptionId) {
    if (!invoiceIterator.hasNext()) {
      return null;
    }

    Invoice invoice = invoiceIterator.next();
    return stripeManager.getInvoiceLineItemsForInvoice(invoice).thenCompose(invoiceLineItems -> {
      Collection<InvoiceLineItem> subscriptionLineItems = invoiceLineItems.stream()
          .filter(invoiceLineItem -> Objects.equals("subscription", invoiceLineItem.getType()))
          .collect(Collectors.toList());
      if (subscriptionLineItems.isEmpty()) {
        return checkNextInvoice(invoiceIterator, subscriptionId);
      }
      if (subscriptionLineItems.size() > 1) {
        throw new IllegalStateException("invoice has more than one subscription; subscriptionId=" + subscriptionId
            + "; count=" + subscriptionLineItems.size());
      }

      InvoiceLineItem subscriptionLineItem = subscriptionLineItems.stream().findAny().get();
      return stripeManager.getProductForPrice(subscriptionLineItem.getPrice().getId()).thenApply(product -> new Receipt(
          Instant.ofEpochSecond(subscriptionLineItem.getPeriod().getEnd()).plus(config.getBadgeGracePeriod()),
          stripeManager.getLevelForProduct(product),
          subscriptionLineItem.getId()));
    });
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
