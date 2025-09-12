/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stripe.Stripe;
import com.stripe.StripeClient;
import com.stripe.exception.CardException;
import com.stripe.exception.IdempotencyException;
import com.stripe.exception.InvalidRequestException;
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
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerVersion;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.ExecutorUtil;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class StripeManager implements CustomerAwareSubscriptionPaymentProcessor {
  private static final Logger logger = LoggerFactory.getLogger(StripeManager.class);
  private static final String METADATA_KEY_LEVEL = "level";
  private static final String METADATA_KEY_CLIENT_PLATFORM = "clientPlatform";

  private final StripeClient stripeClient;
  private final Executor executor;
  private final byte[] idempotencyKeyGenerator;
  private final String boostDescription;
  private final Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod;

  @VisibleForTesting
  StripeManager(
      @Nonnull StripeClient stripeClient,
      @Nonnull Executor executor,
      @Nonnull byte[] idempotencyKeyGenerator,
      @Nonnull String boostDescription,
      @Nonnull Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod) {
    Stripe.setAppInfo("Signal-Server", WhisperServerVersion.getServerVersion());

    this.stripeClient = Objects.requireNonNull(stripeClient);
    this.executor = Objects.requireNonNull(executor);
    this.idempotencyKeyGenerator = Objects.requireNonNull(idempotencyKeyGenerator);
    if (idempotencyKeyGenerator.length == 0) {
      throw new IllegalArgumentException("idempotencyKeyGenerator cannot be empty");
    }
    this.boostDescription = Objects.requireNonNull(boostDescription);
    this.supportedCurrenciesByPaymentMethod = supportedCurrenciesByPaymentMethod;
  }
  public StripeManager(
      @Nonnull String apiKey,
      @Nonnull Executor executor,
      @Nonnull byte[] idempotencyKeyGenerator,
      @Nonnull String boostDescription,
      @Nonnull Map<PaymentMethod, Set<String>> supportedCurrenciesByPaymentMethod) {
    this(new StripeClient(apiKey), executor, idempotencyKeyGenerator, boostDescription, supportedCurrenciesByPaymentMethod);

    if (Strings.isNullOrEmpty(apiKey)) {
      throw new IllegalArgumentException("apiKey cannot be empty");
    }
  }

  @Override
  public PaymentProvider getProvider() {
    return PaymentProvider.STRIPE;
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
  public ProcessorCustomer createCustomer(final byte[] subscriberUser, @Nullable final ClientPlatform clientPlatform) {
    final CustomerCreateParams.Builder builder = CustomerCreateParams.builder()
        .putMetadata("subscriberUser", HexFormat.of().formatHex(subscriberUser));

    if (clientPlatform != null) {
      builder.putMetadata(METADATA_KEY_CLIENT_PLATFORM, clientPlatform.name().toLowerCase());
    }

    try {
      final Customer customer = stripeClient.customers()
          .create(builder.build(), commonOptions(generateIdempotencyKeyForSubscriberUser(subscriberUser)));
      return new ProcessorCustomer(customer.getId(), getProvider());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  public Customer getCustomer(String customerId) {
    CustomerRetrieveParams params = CustomerRetrieveParams.builder().build();
    try {
      return stripeClient.customers().retrieve(customerId, params, commonOptions());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  @Override
  public void setDefaultPaymentMethodForCustomer(String customerId, String paymentMethodId,
      @Nullable String currentSubscriptionId) throws SubscriptionInvalidArgumentsException {
      CustomerUpdateParams params = CustomerUpdateParams.builder()
          .setInvoiceSettings(InvoiceSettings.builder()
              .setDefaultPaymentMethod(paymentMethodId)
              .build())
          .build();
      try {
        stripeClient.customers().update(customerId, params, commonOptions());
      } catch (InvalidRequestException e) {
        // Could happen if the paymentMethodId was bunk or the client didn't actually finish setting it up
        throw new SubscriptionInvalidArgumentsException(e.getMessage());
      } catch (StripeException e) {
        throw new UncheckedIOException(new IOException(e));
      }
  }

  @Override
  public String createPaymentMethodSetupToken(String customerId) {
    SetupIntentCreateParams params = SetupIntentCreateParams.builder()
        .setCustomer(customerId)
        .build();
    try {
      return stripeClient.setupIntents().create(params, commonOptions()).getClientSecret();
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  @Override
  public Set<String> getSupportedCurrenciesForPaymentMethod(final PaymentMethod paymentMethod) {
    return supportedCurrenciesByPaymentMethod.getOrDefault(paymentMethod, Collections.emptySet());
  }

  /**
   * Creates a payment intent. May throw a {@link SubscriptionInvalidAmountException} if stripe rejects the
   * attempt if the amount is too large or too small
   */
  public CompletableFuture<PaymentIntent> createPaymentIntent(final String currency,
      final long amount,
      final long level,
      @Nullable final ClientPlatform clientPlatform) {

    return CompletableFuture.supplyAsync(() -> {
      final PaymentIntentCreateParams.Builder builder = PaymentIntentCreateParams.builder()
          .setAmount(amount)
          .setCurrency(currency.toLowerCase(Locale.ROOT))
          .setDescription(boostDescription)
          .setCaptureMethod(PaymentIntentCreateParams.CaptureMethod.AUTOMATIC)
          .putMetadata("level", Long.toString(level));

      if (clientPlatform != null) {
        builder.putMetadata(METADATA_KEY_CLIENT_PLATFORM, clientPlatform.name().toLowerCase());
      }

      try {
        return stripeClient.paymentIntents().create(builder.build(), commonOptions());
      } catch (StripeException e) {
        final String errorCode = StringUtils.lowerCase(e.getCode(), Locale.ROOT);
        switch (errorCode) {
          case "amount_too_small","amount_too_large" ->
              throw ExceptionUtils.wrap(new SubscriptionInvalidAmountException(errorCode));
          default -> throw new CompletionException(e);
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
  public SubscriptionId createSubscription(String customerId, String priceId, long level,
      long lastSubscriptionCreatedAt)
      throws SubscriptionProcessorException, SubscriptionInvalidArgumentsException {
    // this relies on Stripe's idempotency key to avoid creating more than one subscription if the client
    // retries this request
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
      final Subscription subscription = stripeClient.subscriptions().create(
          params,
          commonOptions(generateIdempotencyKeyForCreateSubscription(customerId, lastSubscriptionCreatedAt)));
      return new SubscriptionId(subscription.getId());
    } catch (IdempotencyException e) {
      throw new SubscriptionInvalidArgumentsException(e.getStripeError().getMessage());
    } catch (CardException e) {
      throw new SubscriptionProcessorException(getProvider(), createChargeFailureFromCardException(e));
    } catch (StripeException e) {
      if ("subscription_payment_intent_requires_action".equals(e.getCode())) {
        throw new SubscriptionPaymentRequiresActionException();
      }
      throw new UncheckedIOException(new IOException(e));
    }
  }

  @Override
  public SubscriptionId updateSubscription(Object subscriptionObj, String priceId, long level, String idempotencyKey)
      throws SubscriptionInvalidArgumentsException, SubscriptionProcessorException {

    final Subscription subscription = getSubscription(subscriptionObj);

    if (getSubscriptionStatus(subscription.getStatus()) == SubscriptionStatus.CANCELED) {
      // If the existing subscription is cancelled, just create a new subscription rather than trying to update a
      // cancelled subscription (which stripe forbids)
      return createSubscription(subscription.getCustomer(), priceId, level, subscription.getCreated());
    }

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
      final Subscription subscription1 = stripeClient.subscriptions().update(subscription.getId(), params,
          commonOptions(generateIdempotencyKeyForSubscriptionUpdate(subscription.getCustomer(), idempotencyKey)));
      return new SubscriptionId(subscription1.getId());
    } catch (IdempotencyException e) {
      throw new SubscriptionInvalidArgumentsException(e.getStripeError().getMessage());
    } catch (CardException e) {
      throw new SubscriptionProcessorException(getProvider(), createChargeFailureFromCardException(e));
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  @Override
  public Object getSubscription(String subscriptionId) {
    SubscriptionRetrieveParams params = SubscriptionRetrieveParams.builder()
        .addExpand("latest_invoice")
        .addExpand("latest_invoice.charge")
        .build();
    try {
      return stripeClient.subscriptions().retrieve(subscriptionId, params, commonOptions());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  @Override
  public void cancelAllActiveSubscriptions(String customerId) {
    final Customer customer = getCustomer(customerId);
      if (customer == null) {
        throw new UncheckedIOException(new IOException("no customer record found for id " + customerId));
      }
      if (StringUtils.isBlank(customer.getId()) || (!customer.getId().equals(customerId))) {
        logger.error("customer ID returned by Stripe ({}) did not match query ({})",  customerId, customer.getSubscriptions());
        throw new UncheckedIOException(new IOException("unexpected customer ID returned by Stripe"));
      }

    final Collection<Subscription> subscriptions = listNonCanceledSubscriptions(customer);
    if (subscriptions.stream()
        .anyMatch(subscription -> !subscription.getCustomer().equals(customerId))) {
      logger.error("Subscription did not match expected customer ID: {}", customerId);
      throw new UncheckedIOException(new IOException("mismatched customer ID"));
    }
    ExecutorUtil.runAll(executor, subscriptions
        .stream()
        .<Runnable>map(subscription -> () -> this.endSubscription(subscription))
        .toList());
  }

  public Collection<Subscription> listNonCanceledSubscriptions(Customer customer) {
    SubscriptionListParams params = SubscriptionListParams.builder()
        .setCustomer(customer.getId())
        .build();
    try {
      return Lists.newArrayList(
          stripeClient.subscriptions().list(params, commonOptions()).autoPagingIterable());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  private Subscription endSubscription(Subscription subscription) {
    final SubscriptionStatus status = SubscriptionStatus.forApiValue(subscription.getStatus());
    return switch (status) {
      // The payment for this period has not processed yet, we should immediately cancel to prevent any payment from
      // going through.
      case UNPAID, PAST_DUE, INCOMPLETE -> cancelSubscriptionImmediately(subscription);
      // Otherwise, set the subscription to cancel at the current period end. If the subscription is active, it may
      // continue to be used until the end of the period.
      default -> cancelSubscriptionAtEndOfCurrentPeriod(subscription);
    };
  }

  private Subscription cancelSubscriptionImmediately(Subscription subscription) {
    SubscriptionCancelParams params = SubscriptionCancelParams.builder().build();
    try {
      return stripeClient.subscriptions().cancel(subscription.getId(), params, commonOptions());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  private Subscription cancelSubscriptionAtEndOfCurrentPeriod(Subscription subscription) {
    SubscriptionUpdateParams params = SubscriptionUpdateParams.builder()
        .setCancelAtPeriodEnd(true)
        .build();
    try {
      return stripeClient.subscriptions().update(subscription.getId(), params, commonOptions());
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  public Collection<SubscriptionItem> getItemsForSubscription(Subscription subscription) {
    try {
      final StripeCollection<SubscriptionItem> subscriptionItems = stripeClient.subscriptionItems().list(
          SubscriptionItemListParams.builder().setSubscription(subscription.getId()).build(), commonOptions());
      return Lists.newArrayList(subscriptionItems.autoPagingIterable());

    } catch (final StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  public Price getPriceForSubscription(Subscription subscription) {
    final Collection<SubscriptionItem> subscriptionItems = getItemsForSubscription(subscription);
    if (subscriptionItems.isEmpty()) {
      throw new IllegalStateException("no items found in subscription " + subscription.getId());
    } else if (subscriptionItems.size() > 1) {
      throw new IllegalStateException(
          "too many items found in subscription " + subscription.getId() + "; items=" + subscriptionItems.size());
    } else {
      return subscriptionItems.stream().findAny().get().getPrice();
    }
  }

  private Product getProductForSubscription(Subscription subscription) {
    return getProductForPrice(getPriceForSubscription(subscription).getId());
  }

  @Override
  public LevelAndCurrency getLevelAndCurrencyForSubscription(Object subscriptionObj) {
    final Subscription subscription = getSubscription(subscriptionObj);

    final Product product = getProductForSubscription(subscription);
    return new LevelAndCurrency(
        getLevelForProduct(product),
        subscription.getCurrency().toLowerCase(Locale.ROOT));
  }

  public long getLevelForPrice(Price price) {
    return getLevelForProduct(getProductForPrice(price.getId()));
  }

  public Product getProductForPrice(String priceId) {
    PriceRetrieveParams params = PriceRetrieveParams.builder().addExpand("product").build();
    try {
      return stripeClient.prices().retrieve(priceId, params, commonOptions()).getProductObject();
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }

  public long getLevelForProduct(Product product) {
    return Long.parseLong(product.getMetadata().get(METADATA_KEY_LEVEL));
  }

  /**
   * Returns the paid invoices within the past 90 days for a subscription ordered by the creation date in descending
   * order (latest first).
   */
  public Collection<Invoice> getPaidInvoicesForSubscription(String subscriptionId, Instant now) {
    InvoiceListParams params = InvoiceListParams.builder()
        .setSubscription(subscriptionId)
        .setStatus(InvoiceListParams.Status.PAID)
        .setCreated(InvoiceListParams.Created.builder()
            .setGte(now.minus(Duration.ofDays(90)).getEpochSecond())
            .build())
        .build();
    try {
      ArrayList<Invoice> invoices = Lists.newArrayList(stripeClient.invoices().list(params, commonOptions())
          .autoPagingIterable());
      invoices.sort(Comparator.comparingLong(Invoice::getCreated).reversed());
      return invoices;
    } catch (StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
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

  private static ChargeFailure createChargeFailureFromCardException(CardException e) {
    return new ChargeFailure(
        StringUtils.defaultIfBlank(e.getDeclineCode(), e.getCode()),
        e.getStripeError().getMessage(),
        null,
        null,
        null
    );
  }

  @Override
  public SubscriptionInformation getSubscriptionInformation(final String subscriptionId) {
    final Subscription subscription = getSubscription(getSubscription(subscriptionId));
    final Price price = getPriceForSubscription(subscription);
    final long level = getLevelForPrice(price);

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
        PaymentProvider.STRIPE,
        paymentMethod,
        paymentProcessing,
        chargeFailure
    );
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
  public ReceiptItem getReceiptItem(String subscriptionId)
      throws SubscriptionPaymentRequiredException, SubscriptionReceiptRequestedForOpenPaymentException {
    final Invoice invoice = getSubscription(getSubscription(subscriptionId)).getLatestInvoiceObject();
    return convertInvoiceToReceipt(invoice, subscriptionId);
  }

  private ReceiptItem convertInvoiceToReceipt(Invoice latestSubscriptionInvoice, String subscriptionId)
      throws SubscriptionReceiptRequestedForOpenPaymentException, SubscriptionPaymentRequiredException {
    if (latestSubscriptionInvoice == null) {
      throw new SubscriptionReceiptRequestedForOpenPaymentException();
    }
    if (StringUtils.equalsIgnoreCase("open", latestSubscriptionInvoice.getStatus())) {
      throw new SubscriptionReceiptRequestedForOpenPaymentException();
    }
    if (!StringUtils.equalsIgnoreCase("paid", latestSubscriptionInvoice.getStatus())) {
      final Charge charge = latestSubscriptionInvoice.getChargeObject();
      if (charge != null && (charge.getFailureCode() != null || charge.getFailureMessage() != null)) {
        // If the charge object has a failure reason we can present to the user, create a detailed exception
        throw new SubscriptionChargeFailurePaymentRequiredException(getProvider(), createChargeFailure(charge));
      } else {
        // Otherwise, return a generic payment required error
        throw new SubscriptionPaymentRequiredException();
      }
    }

    final Collection<InvoiceLineItem> invoiceLineItems = getInvoiceLineItemsForInvoice(latestSubscriptionInvoice);
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
  }

  private ReceiptItem getReceiptForSubscription(InvoiceLineItem subscriptionLineItem, Invoice invoice) {
    final Instant paidAt;
    if (invoice.getStatusTransitions().getPaidAt() != null) {
      paidAt = Instant.ofEpochSecond(invoice.getStatusTransitions().getPaidAt());
    } else {
      logger.warn("No paidAt timestamp exists for paid invoice {}, falling back to start of subscription period",
          invoice.getId());
      paidAt = Instant.ofEpochSecond(subscriptionLineItem.getPeriod().getStart());
    }
    final Product product = getProductForPrice(subscriptionLineItem.getPrice().getId());
    return new ReceiptItem(
        subscriptionLineItem.getId(),
        PaymentTime.periodStart(paidAt),
        getLevelForProduct(product));
  }

  public Collection<InvoiceLineItem> getInvoiceLineItemsForInvoice(Invoice invoice) {
    try {
      final StripeCollection<InvoiceLineItem> lineItems = stripeClient.invoices().lineItems()
          .list(invoice.getId(), commonOptions());
      return Lists.newArrayList(lineItems.autoPagingIterable());
    } catch (final StripeException e) {
      throw new UncheckedIOException(new IOException(e));
    }
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
          throw ExceptionUtils.wrap(new SubscriptionProcessorConflictException());
        }
        return setupIntent.getLatestAttemptObject().getPaymentMethodDetails().getIdeal().getGeneratedSepaDebit();
      } catch (StripeException e) {
        if (e.getStatusCode() == 404) {
          throw ExceptionUtils.wrap(new SubscriptionNotFoundException());
        }
        logger.error("unexpected error from Stripe when retrieving setupIntent {}", setupIntentId, e);
        throw ExceptionUtils.wrap(e);
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
