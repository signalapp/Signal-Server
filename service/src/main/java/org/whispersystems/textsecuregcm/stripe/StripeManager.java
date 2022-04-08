/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.stripe;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stripe.exception.StripeException;
import com.stripe.model.Customer;
import com.stripe.model.Invoice;
import com.stripe.model.InvoiceLineItem;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Price;
import com.stripe.model.Product;
import com.stripe.model.SetupIntent;
import com.stripe.model.Subscription;
import com.stripe.model.SubscriptionItem;
import com.stripe.net.RequestOptions;
import com.stripe.param.CustomerCreateParams;
import com.stripe.param.CustomerRetrieveParams;
import com.stripe.param.CustomerUpdateParams;
import com.stripe.param.CustomerUpdateParams.InvoiceSettings;
import com.stripe.param.InvoiceListParams;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.PriceRetrieveParams;
import com.stripe.param.SetupIntentCreateParams;
import com.stripe.param.SubscriptionCancelParams;
import com.stripe.param.SubscriptionCreateParams;
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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.codec.binary.Hex;
import org.whispersystems.textsecuregcm.util.Conversions;

public class StripeManager {

  private static final String METADATA_KEY_LEVEL = "level";

  private final String apiKey;
  private final Executor executor;
  private final byte[] idempotencyKeyGenerator;
  private final String boostDescription;

  public StripeManager(
      @Nonnull String apiKey,
      @Nonnull Executor executor,
      @Nonnull byte[] idempotencyKeyGenerator,
      @Nonnull String boostDescription) {
    this.apiKey = Objects.requireNonNull(apiKey);
    if (Strings.isNullOrEmpty(apiKey)) {
      throw new IllegalArgumentException("apiKey cannot be empty");
    }
    this.executor = Objects.requireNonNull(executor);
    this.idempotencyKeyGenerator = Objects.requireNonNull(idempotencyKeyGenerator);
    if (idempotencyKeyGenerator.length == 0) {
      throw new IllegalArgumentException("idempotencyKeyGenerator cannot be empty");
    }
    this.boostDescription = Objects.requireNonNull(boostDescription);
  }

  private RequestOptions commonOptions() {
    return commonOptions(null);
  }

  private RequestOptions commonOptions(@Nullable String idempotencyKey) {
    return RequestOptions.builder()
        .setIdempotencyKey(idempotencyKey)
        .setApiKey(apiKey)
        .build();
  }

  public CompletableFuture<Customer> createCustomer(byte[] subscriberUser) {
    return CompletableFuture.supplyAsync(() -> {
      CustomerCreateParams params = CustomerCreateParams.builder()
          .putMetadata("subscriberUser", Hex.encodeHexString(subscriberUser))
          .build();
      try {
        return Customer.create(params, commonOptions(generateIdempotencyKeyForSubscriberUser(subscriberUser)));
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Customer> getCustomer(String customerId) {
    return CompletableFuture.supplyAsync(() -> {
      CustomerRetrieveParams params = CustomerRetrieveParams.builder().build();
      try {
        return Customer.retrieve(customerId, params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Customer> setDefaultPaymentMethodForCustomer(String customerId, String paymentMethodId) {
    return CompletableFuture.supplyAsync(() -> {
      Customer customer = new Customer();
      customer.setId(customerId);
      CustomerUpdateParams params = CustomerUpdateParams.builder()
          .setInvoiceSettings(InvoiceSettings.builder()
              .setDefaultPaymentMethod(paymentMethodId)
              .build())
          .build();
      try {
        return customer.update(params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<SetupIntent> createSetupIntent(String customerId) {
    return CompletableFuture.supplyAsync(() -> {
      SetupIntentCreateParams params = SetupIntentCreateParams.builder()
          .setCustomer(customerId)
          .build();
      try {
        return SetupIntent.create(params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  /**
   * Creates a payment intent. May throw a 400 WebApplicationException if the amount is too small.
   */
  public CompletableFuture<PaymentIntent> createPaymentIntent(String currency, long amount) {
    return CompletableFuture.supplyAsync(() -> {
      PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
          .setAmount(amount)
          .setCurrency(currency.toLowerCase(Locale.ROOT))
          .setDescription(boostDescription)
          .build();
      try {
        return PaymentIntent.create(params, commonOptions());
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

  public CompletableFuture<PaymentIntent> getPaymentIntent(String paymentIntentId) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return PaymentIntent.retrieve(paymentIntentId, commonOptions());
      } catch (StripeException e) {
        if (e.getStatusCode() == 404) {
          return null;
        } else {
          throw new CompletionException(e);
        }
      }
    }, executor);
  }

  public CompletableFuture<Subscription> createSubscription(String customerId, String priceId, long level, long lastSubscriptionCreatedAt) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionCreateParams params = SubscriptionCreateParams.builder()
          .setCustomer(customerId)
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
        return Subscription.create(params, commonOptions(generateIdempotencyKeyForCreateSubscription(
            customerId, lastSubscriptionCreatedAt)));
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Subscription> updateSubscription(
      Subscription subscription, String priceId, long level, String idempotencyKey) {
    return CompletableFuture.supplyAsync(() -> {
      List<SubscriptionUpdateParams.Item> items = new ArrayList<>();
      for (final SubscriptionItem item : subscription.getItems().autoPagingIterable(null, commonOptions())) {
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
          .addAllItem(items)
          .build();
      try {
        return subscription.update(params, commonOptions(generateIdempotencyKeyForSubscriptionUpdate(
            subscription.getCustomer(), idempotencyKey)));
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Subscription> getSubscription(String subscriptionId) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionRetrieveParams params = SubscriptionRetrieveParams.builder()
          .addExpand("latest_invoice")
          .addExpand("latest_invoice.charge")
          .build();
      try {
        return Subscription.retrieve(subscriptionId, params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Collection<Subscription>> listNonCanceledSubscriptions(Customer customer) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionListParams params = SubscriptionListParams.builder()
          .setCustomer(customer.getId())
          .build();
      try {
        return Lists.newArrayList(Subscription.list(params, commonOptions()).autoPagingIterable(null, commonOptions()));
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Subscription> cancelSubscriptionImmediately(Subscription subscription) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionCancelParams params = SubscriptionCancelParams.builder().build();
      try {
        return subscription.cancel(params, commonOptions());
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
        return subscription.update(params, commonOptions());
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Collection<SubscriptionItem>> getItemsForSubscription(Subscription subscription) {
    return CompletableFuture.supplyAsync(
        () -> Lists.newArrayList(subscription.getItems().autoPagingIterable(null, commonOptions())),
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

  public CompletableFuture<Product> getProductForSubscription(Subscription subscription) {
    return getPriceForSubscription(subscription).thenCompose(price -> getProductForPrice(price.getId()));
  }

  public CompletableFuture<Long> getLevelForSubscription(Subscription subscription) {
    return getProductForSubscription(subscription).thenApply(this::getLevelForProduct);
  }

  public CompletableFuture<Long> getLevelForPrice(Price price) {
    return getProductForPrice(price.getId()).thenApply(this::getLevelForProduct);
  }

  public CompletableFuture<Product> getProductForPrice(String priceId) {
    return CompletableFuture.supplyAsync(() -> {
      PriceRetrieveParams params = PriceRetrieveParams.builder().addExpand("product").build();
      try {
        return Price.retrieve(priceId, params, commonOptions()).getProductObject();
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
        ArrayList<Invoice> invoices = Lists.newArrayList(Invoice.list(params, commonOptions())
            .autoPagingIterable(null, commonOptions()));
        invoices.sort(Comparator.comparingLong(Invoice::getCreated).reversed());
        return invoices;
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Invoice> getLatestInvoiceForSubscription(String subscriptionId) {
    return CompletableFuture.supplyAsync(() -> {
      SubscriptionRetrieveParams params = SubscriptionRetrieveParams.builder()
          .addExpand("latest_invoice")
          .build();
      try {
        return Subscription.retrieve(subscriptionId, params, commonOptions()).getLatestInvoiceObject();
      } catch (StripeException e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  public CompletableFuture<Collection<InvoiceLineItem>> getInvoiceLineItemsForInvoice(Invoice invoice) {
    return CompletableFuture.supplyAsync(
        () -> Lists.newArrayList(invoice.getLines().autoPagingIterable(null, commonOptions())), executor);
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
