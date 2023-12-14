/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stripe.StripeClient;
import com.stripe.exception.CardException;
import com.stripe.exception.StripeException;
import com.stripe.model.Charge;
import com.stripe.model.Customer;
import com.stripe.model.Invoice;
import com.stripe.model.InvoiceLineItem;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Price;
import com.stripe.model.Product;
import com.stripe.model.SetupIntent;
import com.stripe.model.StripeCollection;
import com.stripe.model.Subscription;
import com.stripe.model.SubscriptionItem;
import com.stripe.net.RequestOptions;
import com.stripe.param.CustomerCreateParams;
import com.stripe.param.CustomerRetrieveParams;
import com.stripe.param.CustomerUpdateParams;
import com.stripe.param.CustomerUpdateParams.InvoiceSettings;
import com.stripe.param.InvoiceListParams;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.PaymentIntentRetrieveParams;
import com.stripe.param.PriceRetrieveParams;
import com.stripe.param.SetupIntentCreateParams;
import com.stripe.param.SetupIntentRetrieveParams;
import com.stripe.param.SubscriptionCancelParams;
import com.stripe.param.SubscriptionCreateParams;
import com.stripe.param.SubscriptionItemListParams;
import com.stripe.param.SubscriptionListParams;
import com.stripe.param.SubscriptionRetrieveParams;
import com.stripe.param.SubscriptionUpdateParams;
import com.stripe.param.SubscriptionUpdateParams.BillingCycleAnchor;
import com.stripe.param.SubscriptionUpdateParams.ProrationBehavior;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Conversions;

public class StripeManager implements SubscriptionProcessorManager {
  private static final Logger logger = LoggerFactory.getLogger(StripeManager.class);
  private static final String METADATA_KEY_LEVEL = "level";

  private final StripeClient stripeClient;
  private final Executor executor;
  private final byte[] idempotencyKeyGenerator;
  private final String boostDescription;
  private final Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod;

  public StripeManager(
      @Nonnull String apiKey,
      @Nonnull Executor executor,
      @Nonnull byte[] idempotencyKeyGenerator,
      @Nonnull String boostDescription,
      @Nonnull Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod) {
    if (Strings.isNullOrEmpty(apiKey)) {
      throw new IllegalArgumentException("apiKey cannot be empty");
    }
    this.stripeClient = new StripeClient(apiKey);
    this.executor = Objects.requireNonNull(executor);
    this.idempotencyKeyGenerator = Objects.requireNonNull(idempotencyKeyGenerator);
    if (idempotencyKeyGenerator.length == 0) {
      throw new IllegalArgumentException("idempotencyKeyGenerator cannot be empty");
    }
    this.boostDescription = Objects.requireNonNull(boostDescription);
    this.supportedCurrenciesByPaymentMethod = supportedCurrenciesByPaymentMethod;
  }

  @Override
  public SubscriptionProcessor getProcessor() {
    return SubscriptionProcessor.STRIPE;
  }

  @Override
  public boolean supportsPaymentMethod(PaymentMethod paymentMethod) {
    return paymentMethod == PaymentMethod.CARD
        || paymentMethod == PaymentMethod.SEPA_DEBIT
        || paymentMethod == PaymentMethod.IDEAL;
  }

  private RequestOptions commonOptions() {
    return commonOptions(null);
  }

  private RequestOptions commonOptions(@Nullable String idempotencyKey) {
    return RequestOptions.builder()
        .setIdempotencyKey(idempotencyKey)
        .build();
  }

  @Override
  public CompletableFuture<ProcessorCustomer> createCustomer(byte[] subscriberUser) {
    return CompletableFuture.supplyAsync(() -> {
          CustomerCreateParams params = CustomerCreateParams.builder()
              .putMetadata("subscriberUser", HexFormat.of().formatHex(subscriberUser))
              .build();
          try {
            return stripeClient.customers()
                .create(params, commonOptions(generateIdempotencyKeyForSubscriberUser(subscriberUser)));
          } catch (StripeException e) {
            throw new CompletionException(e);
          }
        }, executor)
        .thenApply(customer -> new ProcessorCustomer(customer.getId(), getProcessor()));
  }

  public CompletableFuture<Customer> getCustomer(String customerId) {
    return CompletableFuture.supplyAsync(() -> {
      CustomerRetrieveParams params = CustomerRetrieveParams.builder().build();
      try {
        return stripeClient.customers().retrieve(customerId, params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  @Override
  public CompletableFuture<Void> setDefaultPaymentMethodForCustomer(String customerId, String paymentMethodId,
      @Nullable String currentSubscriptionId) {
    return CompletableFuture.supplyAsync(() -> {
      CustomerUpdateParams params = CustomerUpdateParams.builder()
          .setInvoiceSettings(InvoiceSettings.builder()
              .setDefaultPaymentMethod(paymentMethodId)
              .build())
          .build();
      try {
        stripeClient.customers().update(customerId, params, commonOptions());
        return null;
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  @Override
  public CompletableFuture<String> createPaymentMethodSetupToken(String customerId) {
    return CompletableFuture.supplyAsync(() -> {
          SetupIntentCreateParams params = SetupIntentCreateParams.builder()
              .setCustomer(customerId)
              .build();
          try {
            return stripeClient.setupIntents().create(params, commonOptions());
          } catch (StripeException e) {
            throw new CompletionException(e);
          }
        }, executor)
        .thenApply(SetupIntent::getClientSecret);
  }

  @Override
  public Set<String> getSupportedCurrenciesForPaymentMethod(final PaymentMethod paymentMethod) {
    return supportedCurrenciesByPaymentMethod.getOrDefault(paymentMethod, Collections.emptySet());
  }

  /**
   * Creates a payment intent. May throw a 400 WebApplicationException if the amount is too small.
   */
  public CompletableFuture<PaymentIntent> createPaymentIntent(String currency, long amount, long level) {
    return CompletableFuture.supplyAsync(() -> {
      PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
          .setAmount(amount)
          .setCurrency(currency.toLowerCase(Locale.ROOT))
          .setDescription(boostDescription)
          .putMetadata("level", Long.toString(level))
          .build();
      try {
        return stripeClient.paymentIntents().create(params, commonOptions());
      } catch (StripeException e) {
        if ("amount_too_small".equalsIgnoreCase(e.getCode())) {
          throw new WebApplicationException(Response
              .status(Status.BAD_REQUEST)
              .entity(Map.of("error", "amount_too_small"))
              .build());
        } else {
          throw new CompletionException(e);
        }
      }
    }, executor);
  }

  public CompletableFuture<PaymentDetails> getPaymentDetails(String paymentIntentId) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        final PaymentIntentRetrieveParams params = PaymentIntentRetrieveParams.builder()
            .addExpand("latest_charge").build();
        final PaymentIntent paymentIntent = stripeClient.paymentIntents().retrieve(paymentIntentId, params, commonOptions());

        ChargeFailure chargeFailure = null;
        if (paymentIntent.getLatestChargeObject() != null) {
          final Charge charge = paymentIntent.getLatestChargeObject();
          if (charge.getFailureCode() != null || charge.getFailureMessage() != null) {
            chargeFailure = createChargeFailure(charge);
          }
        }

        return new PaymentDetails(paymentIntent.getId(),
            paymentIntent.getMetadata() == null ? Collections.emptyMap() : paymentIntent.getMetadata(),
            getPaymentStatusForStatus(paymentIntent.getStatus()),
            Instant.ofEpochSecond(paymentIntent.getCreated()),
            chargeFailure);
      } catch (StripeException e) {
        if (e.getStatusCode() == 404) {
          return null;
        } else {
          throw new CompletionException(e);
        }
      }
    }, executor);
  }

  private static PaymentStatus getPaymentStatusForStatus(String status) {
    return switch (status.toLowerCase(Locale.ROOT)) {
      case "processing" -> PaymentStatus.PROCESSING;
      case "succeeded" -> PaymentStatus.SUCCEEDED;
      default -> PaymentStatus.UNKNOWN;
    };
  }

  private static SubscriptionStatus getSubscriptionStatus(final String status) {
    return SubscriptionStatus.forApiValue(status);
  }

  @Override
  public CompletableFuture<SubscriptionId> createSubscription(String customerId, String priceId, long level,
      long lastSubscriptionCreatedAt) {
    // this relies on Stripe's idempotency key to avoid creating more than one subscription if the client
    // retries this request
    return CompletableFuture.supplyAsync(() -> {
          SubscriptionCreateParams params = SubscriptionCreateParams.builder()
              .setCustomer(customerId)
              .setOffSession(true)
              .setPaymentBehavior(SubscriptionCreateParams.PaymentBehavior.ERROR_IF_INCOMPLETE)
              .addItem(SubscriptionCreateParams.Item.builder()
                  .setPrice(priceId)
                  .build())
              .putMetadata(METADATA_KEY_LEVEL, Long.toString(level))
              .build();
          try {
            // the idempotency key intentionally excludes priceId
            //
            // If the client tells the server several times in a row before the initial creation of a subscription to
            // create a subscription, we want to ensure only one gets created.
            return stripeClient.subscriptions()
                .create(params, commonOptions(generateIdempotencyKeyForCreateSubscription(
                customerId, lastSubscriptionCreatedAt)));
          } catch (StripeException e) {

            if (e instanceof CardException ce) {
              throw new CompletionException(
                  new SubscriptionProcessorException(getProcessor(), createChargeFailureFromCardException(e, ce)));
            }

            throw new CompletionException(e);
          }
        }, executor)
        .thenApply(subscription -> new SubscriptionId(subscription.getId()));
  }

  @Override
  public CompletableFuture<SubscriptionId> updateSubscription(
          Object subscriptionObj, String priceId, long level, String idempotencyKey) {

    final Subscription subscription = getSubscription(subscriptionObj);

    return CompletableFuture.supplyAsync(() -> {
          List<SubscriptionUpdateParams.Item> items = new ArrayList<>();
          try {
            final StripeCollection<SubscriptionItem> subscriptionItems = stripeClient.subscriptionItems()
                .list(SubscriptionItemListParams.builder().setSubscription(subscription.getId()).build(),
                    commonOptions());
            for (final SubscriptionItem item : subscriptionItems.autoPagingIterable()) {
              items.add(SubscriptionUpdateParams.Item.builder()
                  .setId(item.getId())
                  .setDeleted(true)
                  .build());
            }
            items.add(SubscriptionUpdateParams.Item.builder()
                .setPrice(priceId)
                .build());
            SubscriptionUpdateParams params = SubscriptionUpdateParams.builder()
                .putMetadata(METADATA_KEY_LEVEL, Long.toString(level))

                // since badge redemption is untrackable by design and unrevokable, subscription changes must be immediate and
                // not prorated
                .setProrationBehavior(ProrationBehavior.NONE)
                .setBillingCycleAnchor(BillingCycleAnchor.NOW)
                .setOffSession(true)
                .setPaymentBehavior(SubscriptionUpdateParams.PaymentBehavior.ERROR_IF_INCOMPLETE)
                .addAllItem(items)
                .build();
            return stripeClient.subscriptions().update(subscription.getId(), params,
                commonOptions(
                    generateIdempotencyKeyForSubscriptionUpdate(subscription.getCustomer(), idempotencyKey)));
          } catch (StripeException e) {

            if (e instanceof CardException ce) {
              throw new CompletionException(
                  new SubscriptionProcessorException(getProcessor(), createChargeFailureFromCardException(e, ce)));
            }
            throw new CompletionException(e);
          }

        }, executor)
        .thenApply(subscription1 -> new SubscriptionId(subscription1.getId()));
  }

  public CompletableFuture<Object> getSubscription(String subscriptionId) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionRetrieveParams params = SubscriptionRetrieveParams.builder()
          .addExpand("latest_invoice")
          .addExpand("latest_invoice.charge")
          .build();
      try {
        return stripeClient.subscriptions().retrieve(subscriptionId, params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Void> cancelAllActiveSubscriptions(String customerId) {
    return getCustomer(customerId).thenCompose(customer -> {
      if (customer == null) {
        throw new InternalServerErrorException(
            "no customer record found for id " + customerId);
      }
      return listNonCanceledSubscriptions(customer);
    }).thenCompose(subscriptions -> {
      @SuppressWarnings("unchecked")
      CompletableFuture<Subscription>[] futures = (CompletableFuture<Subscription>[]) subscriptions.stream()
          .map(this::cancelSubscriptionAtEndOfCurrentPeriod).toArray(CompletableFuture[]::new);
      return CompletableFuture.allOf(futures);
    });
  }

  public CompletableFuture<Collection<Subscription>> listNonCanceledSubscriptions(Customer customer) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionListParams params = SubscriptionListParams.builder()
          .setCustomer(customer.getId())
          .build();
      try {
        return Lists.newArrayList(
            stripeClient.subscriptions().list(params, commonOptions()).autoPagingIterable(null, commonOptions()));
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Subscription> cancelSubscriptionImmediately(Subscription subscription) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionCancelParams params = SubscriptionCancelParams.builder().build();
      try {
        return stripeClient.subscriptions().cancel(subscription.getId(), params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Subscription> cancelSubscriptionAtEndOfCurrentPeriod(Subscription subscription) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionUpdateParams params = SubscriptionUpdateParams.builder()
          .setCancelAtPeriodEnd(true)
          .build();
      try {
        return stripeClient.subscriptions().update(subscription.getId(), params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Collection<SubscriptionItem>> getItemsForSubscription(Subscription subscription) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final StripeCollection<SubscriptionItem> subscriptionItems = stripeClient.subscriptionItems().list(
                SubscriptionItemListParams.builder().setSubscription(subscription.getId()).build(), commonOptions());
            return Lists.newArrayList(subscriptionItems.autoPagingIterable(null, commonOptions()));

          } catch (final StripeException e) {
            throw new CompletionException(e);
          }
        },
        executor);
  }

  public CompletableFuture<Price> getPriceForSubscription(Subscription subscription) {
    return getItemsForSubscription(subscription).thenApply(subscriptionItems -> {
      if (subscriptionItems.isEmpty()) {
        throw new IllegalStateException("no items found in subscription " + subscription.getId());
      } else if (subscriptionItems.size() > 1) {
        throw new IllegalStateException(
            "too many items found in subscription " + subscription.getId() + "; items=" + subscriptionItems.size());
      } else {
        return subscriptionItems.stream().findAny().get().getPrice();
      }
    });
  }

  private CompletableFuture<Product> getProductForSubscription(Subscription subscription) {
    return getPriceForSubscription(subscription).thenCompose(price -> getProductForPrice(price.getId()));
  }

  @Override
  public CompletableFuture<LevelAndCurrency> getLevelAndCurrencyForSubscription(Object subscriptionObj) {
    final Subscription subscription = getSubscription(subscriptionObj);

    return getProductForSubscription(subscription).thenApply(
        product -> new LevelAndCurrency(getLevelForProduct(product), subscription.getCurrency().toLowerCase(
            Locale.ROOT)));
  }

  public CompletableFuture<Long> getLevelForPrice(Price price) {
    return getProductForPrice(price.getId()).thenApply(this::getLevelForProduct);
  }

  public CompletableFuture<Product> getProductForPrice(String priceId) {
    return CompletableFuture.supplyAsync(() -> {
      PriceRetrieveParams params = PriceRetrieveParams.builder().addExpand("product").build();
      try {
        return stripeClient.prices().retrieve(priceId, params, commonOptions()).getProductObject();
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public long getLevelForProduct(Product product) {
    return Long.parseLong(product.getMetadata().get(METADATA_KEY_LEVEL));
  }

  /**
   * Returns the paid invoices within the past 90 days for a subscription ordered by the creation date in descending
   * order (latest first).
   */
  public CompletableFuture<Collection<Invoice>> getPaidInvoicesForSubscription(String subscriptionId, Instant now) {
    return CompletableFuture.supplyAsync(() -> {
      InvoiceListParams params = InvoiceListParams.builder()
          .setSubscription(subscriptionId)
          .setStatus(InvoiceListParams.Status.PAID)
          .setCreated(InvoiceListParams.Created.builder()
              .setGte(now.minus(Duration.ofDays(90)).getEpochSecond())
              .build())
          .build();
      try {
        ArrayList<Invoice> invoices = Lists.newArrayList(stripeClient.invoices().list(params, commonOptions())
                .autoPagingIterable(null, commonOptions()));
        invoices.sort(Comparator.comparingLong(Invoice::getCreated).reversed());
        return invoices;
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  private static ChargeFailure createChargeFailure(final Charge charge) {
    Charge.Outcome outcome = charge.getOutcome();
    return new ChargeFailure(
        charge.getFailureCode(),
        charge.getFailureMessage(),
        outcome != null ? outcome.getNetworkStatus() : null,
        outcome != null ? outcome.getReason() : null,
        outcome != null ? outcome.getType() : null);
  }

  private static ChargeFailure createChargeFailureFromCardException(StripeException e, CardException ce) {
    return new ChargeFailure(
        StringUtils.defaultIfBlank(ce.getDeclineCode(), ce.getCode()),
        e.getStripeError().getMessage(),
        null,
        null,
        null
    );
  }

  @Override
  public CompletableFuture<SubscriptionInformation> getSubscriptionInformation(Object subscriptionObj) {

    final Subscription subscription = getSubscription(subscriptionObj);

    return getPriceForSubscription(subscription).thenCompose(price ->
            getLevelForPrice(price).thenApply(level -> {
              ChargeFailure chargeFailure = null;
              boolean paymentProcessing = false;
              PaymentMethod paymentMethod = null;

              if (subscription.getLatestInvoiceObject() != null) {
                final Invoice invoice = subscription.getLatestInvoiceObject();
                paymentProcessing = "open".equals(invoice.getStatus());

                if (invoice.getChargeObject() != null) {
                  final Charge charge = invoice.getChargeObject();
                  if (charge.getFailureCode() != null || charge.getFailureMessage() != null) {
                    chargeFailure = createChargeFailure(charge);
                  }

                  if (charge.getPaymentMethodDetails() != null
                      && charge.getPaymentMethodDetails().getType() != null) {
                    paymentMethod = getPaymentMethodFromStripeString(charge.getPaymentMethodDetails().getType(), invoice.getId());
                  }
                }
              }

              return new SubscriptionInformation(
                  new SubscriptionPrice(price.getCurrency().toUpperCase(Locale.ROOT), price.getUnitAmountDecimal()),
                  level,
                  Instant.ofEpochSecond(subscription.getBillingCycleAnchor()),
                  Instant.ofEpochSecond(subscription.getCurrentPeriodEnd()),
                  Objects.equals(subscription.getStatus(), "active"),
                  subscription.getCancelAtPeriodEnd(),
                  getSubscriptionStatus(subscription.getStatus()),
                  paymentMethod,
                  paymentProcessing,
                  chargeFailure
              );
            }));
  }

  private static PaymentMethod getPaymentMethodFromStripeString(final String paymentMethodString, final String invoiceId) {
    return switch (paymentMethodString) {
      case "sepa_debit" -> PaymentMethod.SEPA_DEBIT;
      case "card" -> PaymentMethod.CARD;
      default -> {
        logger.error("Unexpected payment method from Stripe: {}, invoice id: {}", paymentMethodString, invoiceId);
        yield PaymentMethod.UNKNOWN;
      }
    };
  }

  private Subscription getSubscription(Object subscriptionObj) {
    if (!(subscriptionObj instanceof final Subscription subscription)) {
      throw new IllegalArgumentException("invalid subscription object: " + subscriptionObj.getClass().getName());
    }

    return subscription;
  }

  @Override
  public CompletableFuture<ReceiptItem> getReceiptItem(String subscriptionId) {
    return getSubscription(subscriptionId)
        .thenApply(stripeSubscription -> getSubscription(stripeSubscription).getLatestInvoiceObject())
        .thenCompose(invoice -> convertInvoiceToReceipt(invoice, subscriptionId));
  }

  private CompletableFuture<ReceiptItem> convertInvoiceToReceipt(Invoice latestSubscriptionInvoice, String subscriptionId) {
    if (latestSubscriptionInvoice == null) {
      throw new WebApplicationException(Status.NO_CONTENT);
    }
    if (StringUtils.equalsIgnoreCase("open", latestSubscriptionInvoice.getStatus())) {
      throw new WebApplicationException(Status.NO_CONTENT);
    }
    if (!StringUtils.equalsIgnoreCase("paid", latestSubscriptionInvoice.getStatus())) {
      final Response.ResponseBuilder responseBuilder = Response.status(Status.PAYMENT_REQUIRED);
      if (latestSubscriptionInvoice.getChargeObject() != null) {
        final Charge charge = latestSubscriptionInvoice.getChargeObject();
        if (charge.getFailureCode() != null || charge.getFailureMessage() != null) {
          responseBuilder.entity(Map.of("chargeFailure", createChargeFailure(charge)));
        }
      }
      throw new WebApplicationException(responseBuilder.build());
    }

    return getInvoiceLineItemsForInvoice(latestSubscriptionInvoice).thenCompose(invoiceLineItems -> {
      Collection<InvoiceLineItem> subscriptionLineItems = invoiceLineItems.stream()
          .filter(invoiceLineItem -> Objects.equals("subscription", invoiceLineItem.getType()))
          .toList();
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
      return getReceiptForSubscription(subscriptionLineItem, latestSubscriptionInvoice);
    });
  }

  private CompletableFuture<ReceiptItem> getReceiptForSubscription(InvoiceLineItem subscriptionLineItem,
      Invoice invoice) {
    final Instant paidAt;
    if (invoice.getStatusTransitions().getPaidAt() != null) {
      paidAt = Instant.ofEpochSecond(invoice.getStatusTransitions().getPaidAt());
    } else {
      logger.warn("No paidAt timestamp exists for paid invoice {}, falling back to start of subscription period", invoice.getId());
      paidAt = Instant.ofEpochSecond(subscriptionLineItem.getPeriod().getStart());
    }
    return getProductForPrice(subscriptionLineItem.getPrice().getId()).thenApply(product -> new ReceiptItem(
        subscriptionLineItem.getId(),
        paidAt,
        getLevelForProduct(product)));
  }

  public CompletableFuture<Collection<InvoiceLineItem>> getInvoiceLineItemsForInvoice(Invoice invoice) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final StripeCollection<InvoiceLineItem> lineItems = stripeClient.invoices().lineItems()
                .list(invoice.getId(), commonOptions());
            return Lists.newArrayList(lineItems.autoPagingIterable(null, commonOptions()));
          } catch (final StripeException e) {
            throw new CompletionException(e);
          }
        }, executor);
  }

  public CompletableFuture<String> getGeneratedSepaIdFromSetupIntent(String setupIntentId) {
    return CompletableFuture.supplyAsync(() -> {
      SetupIntentRetrieveParams params = SetupIntentRetrieveParams.builder()
          .addExpand("latest_attempt")
          .build();
      try {
        final SetupIntent setupIntent = stripeClient.setupIntents().retrieve(setupIntentId, params, commonOptions());
        if (setupIntent.getLatestAttemptObject() == null
            || setupIntent.getLatestAttemptObject().getPaymentMethodDetails() == null
            || setupIntent.getLatestAttemptObject().getPaymentMethodDetails().getIdeal() == null
            || setupIntent.getLatestAttemptObject().getPaymentMethodDetails().getIdeal().getGeneratedSepaDebit() == null) {
          // This usually indicates that the client has made requests out of order, either by not confirming
          // the SetupIntent or not having the user authorize the transaction.
          logger.debug("setupIntent {} missing expected fields", setupIntentId);
          throw new ClientErrorException(Status.CONFLICT);
        }
        return setupIntent.getLatestAttemptObject().getPaymentMethodDetails().getIdeal().getGeneratedSepaDebit();
      } catch (StripeException e) {
        if (e.getStatusCode() == 404) {
          throw new NotFoundException();
        }
        logger.error("unexpected error from Stripe when retrieving setupIntent {}", setupIntentId, e);
        throw new CompletionException(e);
      }
    }, executor);
  }

  /**
   * We use a client generated idempotency key for subscription updates due to not being able to distinguish between a
   * call to update to level 2, then back to level 1, then back to level 2. If this all happens within Stripe's
   * idempotency window the subsequent update call would not happen unless we get some indication from the client that
   * it is intentionally sending a repeat of the update to level 2 request because user is changing again, so in this
   * case we derive idempotency from the client.
   */
  private String generateIdempotencyKeyForSubscriptionUpdate(String customerId, String idempotencyKey) {
    return generateIdempotencyKey("subscriptionUpdate", mac -> {
      mac.update(customerId.getBytes(StandardCharsets.UTF_8));
      mac.update(idempotencyKey.getBytes(StandardCharsets.UTF_8));
    });
  }

  private String generateIdempotencyKeyForSubscriberUser(byte[] subscriberUser) {
    return generateIdempotencyKey("subscriberUser", mac -> mac.update(subscriberUser));
  }

  private String generateIdempotencyKeyForCreateSubscription(String customerId, long lastSubscriptionCreatedAt) {
    return generateIdempotencyKey("customerId", mac -> {
      mac.update(customerId.getBytes(StandardCharsets.UTF_8));
      mac.update(Conversions.longToByteArray(lastSubscriptionCreatedAt));
    });
  }

  private String generateIdempotencyKey(String type, Consumer<Mac> byteConsumer) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(idempotencyKeyGenerator, "HmacSHA256"));
      mac.update(type.getBytes(StandardCharsets.UTF_8));
      byteConsumer.accept(mac);
      return Base64.getUrlEncoder().encodeToString(mac.doFinal());
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }
}
