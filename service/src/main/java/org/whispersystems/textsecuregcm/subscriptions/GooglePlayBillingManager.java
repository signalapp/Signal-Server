/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisherRequest;
import com.google.api.services.androidpublisher.AndroidPublisherScopes;
import com.google.api.services.androidpublisher.model.AutoRenewingPlan;
import com.google.api.services.androidpublisher.model.Money;
import com.google.api.services.androidpublisher.model.ProductLineItem;
import com.google.api.services.androidpublisher.model.ProductOfferDetails;
import com.google.api.services.androidpublisher.model.ProductPurchaseV2;
import com.google.api.services.androidpublisher.model.PurchaseStateContext;
import com.google.api.services.androidpublisher.model.SubscriptionPurchaseLineItem;
import com.google.api.services.androidpublisher.model.SubscriptionPurchaseV2;
import com.google.api.services.androidpublisher.model.SubscriptionPurchasesAcknowledgeRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;

/**
 * Manages subscriptions made with the Play Billing API
 * <p>
 * Clients create a subscription using Play Billing directly, and then notify us about their subscription with their
 * <a href="https://developer.android.com/google/play/billing/#concepts">purchaseToken</a>. This class provides methods
 * for
 * <ul>
 * <li> <a href="https://developer.android.com/google/play/billing/security#verify">validating purchaseTokens</a> </li>
 * <li> <a href="https://developer.android.com/google/play/billing/integrate#subscriptions">acknowledging purchaseTokens</a> </li>
 * <li> querying the current status of a token's underlying subscription </li>
 * </ul>
 */
public class GooglePlayBillingManager implements SubscriptionPaymentProcessor, OneTimePaymentProcessor {

  private static final Logger logger = LoggerFactory.getLogger(GooglePlayBillingManager.class);

  private final AndroidPublisher androidPublisher;
  private final String packageName;
  private final Map<String, ReceiptLevel> productIdToLevel;
  private final Clock clock;

  private static final String VALIDATE_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "validate");
  private static final String CANCEL_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "cancel");
  private static final String GET_RECEIPT_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "getReceipt");


  public GooglePlayBillingManager(
      final InputStream credentialsStream,
      final String packageName,
      final String applicationName,
      final Map<String, ReceiptLevel> productIdToLevel)
      throws GeneralSecurityException, IOException {
    this(new AndroidPublisher.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(GoogleCredentials
                .fromStream(credentialsStream)
                .createScoped(AndroidPublisherScopes.ANDROIDPUBLISHER)))
            .setApplicationName(applicationName)
            .build(),
        Clock.systemUTC(), packageName, productIdToLevel);
  }

  @VisibleForTesting
  GooglePlayBillingManager(
      final AndroidPublisher androidPublisher,
      final Clock clock,
      final String packageName,
      final Map<String, ReceiptLevel> productIdToLevel) {
    this.clock = clock;
    this.androidPublisher = androidPublisher;
    this.productIdToLevel = productIdToLevel;
    this.packageName = packageName;
  }

  @Override
  public PaymentProvider getProvider() {
    return PaymentProvider.GOOGLE_PLAY_BILLING;
  }

  /**
   * Represents a valid purchaseToken that should be durably stored and then acknowledged with
   * {@link #acknowledgePurchase()}
   */
  public class ValidatedToken {

    private final long level;
    private final String productId;
    private final String purchaseToken;
    // If false, the purchase has already been acknowledged
    private final boolean requiresAck;

    ValidatedToken(final long level, final String productId, final String purchaseToken, final boolean requiresAck) {
      this.level = level;
      this.productId = productId;
      this.purchaseToken = purchaseToken;
      this.requiresAck = requiresAck;
    }

    /**
     * Acknowledge the purchase to the play billing server. If a purchase is never acknowledged, it will eventually be
     * refunded.
     *
     */
    public void acknowledgePurchase()
        throws RateLimitExceededException, SubscriptionNotFoundException {
      if (!requiresAck) {
        // We've already acknowledged this purchase on a previous attempt, nothing to do
        return;
      }
      executeTokenOperation(pub -> pub.purchases().subscriptions()
          .acknowledge(packageName, productId, purchaseToken, new SubscriptionPurchasesAcknowledgeRequest()));
    }

    public long getLevel() {
      return level;
    }
  }

  /**
   * Check if the purchaseToken is valid. If it's valid it should be durably associated with the user's subscriberId and
   * then acknowledged with {@link ValidatedToken#acknowledgePurchase()}
   *
   * @param purchaseToken The play store billing purchaseToken that represents a subscription purchase
   * @return A {@link ValidatedToken} that can be acknowledged
   * @throws RateLimitExceededException            If rate-limited by play-billing
   * @throws SubscriptionNotFoundException        If the provided purchaseToken was not found in play-billing
   * @throws SubscriptionPaymentRequiredException If the purchaseToken exists but is in a state that does not grant the
   *                                               user an entitlement
   */
  public ValidatedToken validateToken(String purchaseToken)
      throws RateLimitExceededException, SubscriptionNotFoundException, SubscriptionPaymentRequiredException {
    final SubscriptionPurchaseV2 subscription = lookupSubscription(purchaseToken);
    final SubscriptionState state = SubscriptionState
        .fromString(subscription.getSubscriptionState())
        .orElse(SubscriptionState.UNSPECIFIED);

    Metrics.counter(VALIDATE_COUNTER_NAME, subscriptionTags(subscription)).increment();

    // We only accept tokens in a state where the user may be entitled to their purchase. This is true even in the
    // CANCELLED state. For example, a user may subscribe for 1 month, then immediately cancel (disabling auto-renew)
    // and then submit their token. In this case they should still be able to retrieve their entitlement.
    // See https://developer.android.com/google/play/billing/integrate#life
    if (state != SubscriptionState.ACTIVE
        && state != SubscriptionState.IN_GRACE_PERIOD
        && state != SubscriptionState.CANCELED) {
      throw new SubscriptionPaymentRequiredException(
          "Cannot acknowledge purchase for subscription in state " + subscription.getSubscriptionState());
    }

    final AcknowledgementState acknowledgementState = AcknowledgementState
        .fromString(subscription.getAcknowledgementState())
        .orElse(AcknowledgementState.UNSPECIFIED);

    final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);
    final ReceiptLevel level = productIdToLevel(purchase.getProductId());

    return new ValidatedToken(level.getValue(), purchase.getProductId(), purchaseToken, requiresAcknowledgement(subscription));
  }


  /**
   * Cancel the subscription. Cancellation stops auto-renewal, but does not refund the user nor cut off access to their
   * entitlement until their current period expires.
   *
   * @param purchaseToken The purchaseToken associated with the subscription
   * @throws RateLimitExceededException If rate-limited by play-billing
   */
  public void cancelAllActiveSubscriptions(String purchaseToken) throws RateLimitExceededException {
    try {
      final SubscriptionPurchaseV2 subscription = lookupSubscription(purchaseToken);
      Metrics.counter(CANCEL_COUNTER_NAME, subscriptionTags(subscription)).increment();

      final SubscriptionState state = SubscriptionState
          .fromString(subscription.getSubscriptionState())
          .orElse(SubscriptionState.UNSPECIFIED);

      if (state == SubscriptionState.CANCELED || state == SubscriptionState.EXPIRED) {
        // already cancelled, nothing to do
        return;
      }
      final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);

      executeTokenOperation(pub ->
          pub.purchases().subscriptions().cancel(packageName, purchase.getProductId(), purchaseToken));
    } catch (SubscriptionNotFoundException e) {
      // If the subscription is not found there is no need to do anything, so we can squash it
    }
  }

  @Override
  public SubscriptionInformation getSubscriptionInformation(final String purchaseToken)
      throws RateLimitExceededException, SubscriptionNotFoundException {

    final SubscriptionPurchaseV2 subscription = lookupSubscription(purchaseToken);
    final SubscriptionPrice price = getSubscriptionPrice(subscription);

    final SubscriptionPurchaseLineItem lineItem = getLineItem(subscription);
    final Optional<Instant> billingCycleAnchor = getStartTime(subscription);
    final Optional<Instant> expiration = getExpiration(lineItem);

    final SubscriptionStatus status = switch (SubscriptionState
        .fromString(subscription.getSubscriptionState())
        .orElse(SubscriptionState.UNSPECIFIED)) {
      // In play terminology CANCELLED is the same as an active subscription with cancelAtPeriodEnd set in Stripe. So
      // it should map to the ACTIVE stripe status.
      case ACTIVE, CANCELED -> SubscriptionStatus.ACTIVE;
      case PENDING -> SubscriptionStatus.INCOMPLETE;
      case ON_HOLD, PAUSED -> SubscriptionStatus.PAST_DUE;
      case IN_GRACE_PERIOD -> SubscriptionStatus.UNPAID;
      // EXPIRED is the equivalent of a Stripe CANCELLED subscription
      case EXPIRED, PENDING_PURCHASE_CANCELED -> SubscriptionStatus.CANCELED;
      case UNSPECIFIED -> SubscriptionStatus.UNKNOWN;
    };

    final boolean autoRenewEnabled = Optional
        .ofNullable(lineItem.getAutoRenewingPlan())
        .map(AutoRenewingPlan::getAutoRenewEnabled) // returns null or false if auto-renew disabled
        .orElse(false);
    return new SubscriptionInformation(
        price,
        productIdToLevel(lineItem.getProductId()).getValue(),
        billingCycleAnchor.orElse(null),
        expiration.orElse(null),
        expiration.map(clock.instant()::isBefore).orElse(false),
        !autoRenewEnabled,
        status,
        PaymentProvider.GOOGLE_PLAY_BILLING,
        PaymentMethod.GOOGLE_PLAY_BILLING,
        false,
        null);
  }

  private SubscriptionPrice getSubscriptionPrice(final SubscriptionPurchaseV2 subscriptionPurchase) {
    final SubscriptionPurchaseLineItem lineItem = getLineItem(subscriptionPurchase);

    // We don't offer pre-paid plans, so autoRenewingPlan must be nonnull
    if (lineItem.getAutoRenewingPlan() == null) {
      throw new UncheckedIOException(new IOException("Subscription purchases must be auto-renewing plans"));
    }
    final Money price = lineItem.getAutoRenewingPlan().getRecurringPrice();
    return new SubscriptionPrice(
        price.getCurrencyCode().toUpperCase(Locale.ROOT),
        SubscriptionCurrencyUtil.convertGoogleMoneyToApiAmount(price));
  }

  @Override
  public ReceiptItem getReceiptItem(String purchaseToken)
      throws RateLimitExceededException, SubscriptionNotFoundException, SubscriptionPaymentRequiredException {
    final SubscriptionPurchaseV2 subscription = lookupSubscription(purchaseToken);

    Metrics.counter(GET_RECEIPT_COUNTER_NAME, subscriptionTags(subscription)).increment();

    final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);
    final Instant expiration = getExpiration(purchase)
        .orElseThrow(() -> new UncheckedIOException(new IOException("Invalid subscription expiration")));

    if (expiration.isBefore(clock.instant())) {
      // We don't need to check any state at this point, just whether the subscription is currently valid. If the
      // subscription is in a grace period, the expiration time will be dynamically extended, see
      // https://developer.android.com/google/play/billing/lifecycle/subscriptions#grace-period
      throw new SubscriptionPaymentRequiredException();
    }

    if (requiresAcknowledgement(subscription)) {
      // We only generate receipts for previously stored tokens. Usually, we acknowledge tokens after storing them.
      // However, it's possible that a client sent us a token, and we successfully stored it, but then failed to
      // acknowledge it. If the client later attempts to create a receipt from that token, we can be confident that
      // they are going to use their entitlement, so we can go ahead and acknowledge it
      logger.info("Tried to fetch receipt for purchase token that was never acknowledged. Acknowledging. orderId: {} latestSuccessfulOrderId: {}, acknowledgementState: {}, canceledStateContext: {}, state: {} ",
          subscription.getLatestOrderId(),
          purchase.getLatestSuccessfulOrderId(),
          subscription.getAcknowledgementState(),
          subscription.getCanceledStateContext(),
          subscription.getSubscriptionState());
      executeTokenOperation(pub -> pub.purchases().subscriptions()
          .acknowledge(packageName, purchase.getProductId(), purchaseToken, new SubscriptionPurchasesAcknowledgeRequest()));
    }

    return new ReceiptItem(
        subscription.getLatestOrderId(),
        PaymentTime.periodEnds(expiration),
        productIdToLevel(purchase.getProductId()).getValue());
  }


  /// @implNote Play consumable purchases must be consumed (or they will eventually be refunded). Retrieving the
  /// PaymentInfo for a purchase also consumes the token. This does not mean subsequent attempts to generate a receipt
  /// will fail, it just means we've told the play store that the purchase is complete. If the client fails after the
  /// acknowledgement, they can just retry with no issue.
  @Override
  public Optional<PaymentDetails> claimOneTimePurchase(final String purchaseToken)
      throws IOException, RateLimitExceededException, SubscriptionInvalidArgumentsException {
    try {
      final ProductPurchaseV2 productPurchaseV2 = executeTokenOperation(
          publisher -> publisher.purchases().productsv2().getproductpurchasev2(packageName, purchaseToken));

      final PaymentStatus paymentStatus = Optional
          .ofNullable(productPurchaseV2.getPurchaseStateContext())
          .map(PurchaseStateContext::getPurchaseState)
          .flatMap(PurchaseState::fromString)
          .map(purchaseState -> switch (purchaseState) {
            case PENDING -> PaymentStatus.PROCESSING;
            case CANCELLED -> PaymentStatus.FAILED;
            case PURCHASED -> PaymentStatus.SUCCEEDED;
            case PURCHASE_STATE_UNSPECIFIED -> PaymentStatus.UNKNOWN;
          })
          .orElse(PaymentStatus.UNKNOWN);

      final ProductLineItem lineItem = getLineItem(productPurchaseV2);
      final String productId = lineItem.getProductId();
      final ReceiptLevel level = productIdToLevel(productId);

      Instant purchaseTime = null;
      if (paymentStatus == PaymentStatus.SUCCEEDED) {
        purchaseTime = parseTimestamp(productPurchaseV2.getPurchaseCompletionTime())
            .orElseThrow(() -> new IOException("Invalid purchase time"));

        final ConsumptionState consumptionState = Optional
            .ofNullable(lineItem.getProductOfferDetails())
            .map(ProductOfferDetails::getConsumptionState)
            .flatMap(ConsumptionState::fromString)
            .orElseThrow(() -> new IllegalStateException("Purchase did not contain a consumption state: " + lineItem.getProductOfferDetails()));

        if (consumptionState == ConsumptionState.YET_TO_BE_CONSUMED) {
          // Mark this token as consumed
          executeTokenOperation(publisher ->
              publisher.purchases().products().consume(packageName, productId, purchaseToken));
        }
      }
      return Optional.of(new PaymentDetails(purchaseToken, level, paymentStatus, purchaseTime, null));
    } catch (SubscriptionNotFoundException e) {
      return Optional.empty();
    }
  }

  private ProductLineItem getLineItem(final ProductPurchaseV2 purchase) throws SubscriptionInvalidArgumentsException {
    final List<ProductLineItem> lineItems = purchase.getProductLineItem();
    if (lineItems == null || lineItems.isEmpty()) {
      throw new SubscriptionInvalidArgumentsException("purchase has no line items");
    }
    if (lineItems.size() > 1) {
      logger.warn("{} line items found for purchase {}, expected 1", lineItems.size(), purchase.getOrderId());
    }
    return lineItems.getFirst();
  }

  interface ApiCall<T> {

    AndroidPublisherRequest<T> req(AndroidPublisher publisher) throws IOException;
  }

  /**
   * Asynchronously execute a synchronous API call on a purchaseToken, mapping expected errors to the appropriate
   * {@link SubscriptionException}
   *
   * @param apiCall An API call that operates on a purchaseToken
   * @param <R>     The result of the API call
   * @return A stage that completes with the result of the API call
   */
  private <R> R executeTokenOperation(final ApiCall<R> apiCall)
      throws RateLimitExceededException, SubscriptionNotFoundException {
    try {
      return apiCall.req(androidPublisher).execute();
    } catch (HttpResponseException e) {
      if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()
          || e.getStatusCode() == Response.Status.GONE.getStatusCode()) {
        throw new SubscriptionNotFoundException();
      }
      if (e.getStatusCode() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
        throw new RateLimitExceededException(null);
      }

      final String details;

      if (e instanceof GoogleJsonResponseException googleJsonResponseException && googleJsonResponseException.getDetails() != null) {
        details = googleJsonResponseException.getDetails().toString();
      } else {
        details = "";
      }

      final String message =
          String.format("Unexpected HTTP status code %s from androidpublisher: %s", e.getStatusCode(), details);
      logger.warn(message);
      throw new UncheckedIOException(new IOException(message));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private SubscriptionPurchaseV2 lookupSubscription(final String purchaseToken)
      throws RateLimitExceededException, SubscriptionNotFoundException {
    return executeTokenOperation(publisher -> publisher.purchases().subscriptionsv2().get(packageName, purchaseToken));
  }

  private ReceiptLevel productIdToLevel(final String productId) {
    final ReceiptLevel level = this.productIdToLevel.get(productId);
    if (level == null) {
      logger.error("productId={} had no associated level", productId);
      // This was a productId a user was able to successfully purchase from our catalog,
      // but we don't know about it. The server's configuration is behind.
      throw new IllegalStateException("no level found for productId " + productId);
    }
    return level;
  }

  private SubscriptionPurchaseLineItem getLineItem(final SubscriptionPurchaseV2 subscription) {
    final List<SubscriptionPurchaseLineItem> lineItems = subscription.getLineItems();
    if (lineItems.isEmpty()) {
      throw new IllegalArgumentException("Subscriptions should have line items");
    }
    if (lineItems.size() > 1) {
      logger.warn("{} line items found for purchase {}, expected 1", lineItems.size(), subscription.getLatestOrderId());
    }
    return lineItems.getFirst();
  }

  private static boolean requiresAcknowledgement(final SubscriptionPurchaseV2 subscription) {
    return switch (AcknowledgementState
        .fromString(subscription.getAcknowledgementState())
        .orElse(AcknowledgementState.UNSPECIFIED)) {
      case ACKNOWLEDGED -> false;
      case PENDING -> true;
      case UNSPECIFIED -> throw new UncheckedIOException(
          new IOException("Invalid acknowledgement state " + subscription.getAcknowledgementState()));
    };
  }

  private Tags subscriptionTags(final SubscriptionPurchaseV2 subscription) {
    final boolean expired = subscription.getLineItems().isEmpty() ||
        getExpiration(getLineItem(subscription)).orElse(Instant.EPOCH).isBefore(clock.instant());
    return Tags.of(
        "expired", Boolean.toString(expired),
        "subscriptionState", subscription.getSubscriptionState(),
        "acknowledgementState", subscription.getAcknowledgementState());
  }

  private Optional<Instant> getStartTime(final SubscriptionPurchaseV2 subscription) {
    return parseTimestamp(subscription.getStartTime());
  }

  private Optional<Instant> getExpiration(final SubscriptionPurchaseLineItem purchaseLineItem) {
    return parseTimestamp(purchaseLineItem.getExpiryTime());
  }

  private Optional<Instant> parseTimestamp(final String timestamp) {
    if (StringUtils.isBlank(timestamp)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Instant.parse(timestamp));
    } catch (DateTimeParseException e) {
      logger.warn("received a timestamp with an invalid format: {}", timestamp);
      return Optional.empty();
    }
  }

  // https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.subscriptionsv2#SubscriptionState
  @VisibleForTesting
  enum SubscriptionState {
    UNSPECIFIED("SUBSCRIPTION_STATE_UNSPECIFIED"),
    PENDING("SUBSCRIPTION_STATE_PENDING"),
    ACTIVE("SUBSCRIPTION_STATE_ACTIVE"),
    PAUSED("SUBSCRIPTION_STATE_PAUSED"),
    IN_GRACE_PERIOD("SUBSCRIPTION_STATE_IN_GRACE_PERIOD"),
    ON_HOLD("SUBSCRIPTION_STATE_ON_HOLD"),
    CANCELED("SUBSCRIPTION_STATE_CANCELED"),
    EXPIRED("SUBSCRIPTION_STATE_EXPIRED"),
    PENDING_PURCHASE_CANCELED("SUBSCRIPTION_STATE_PENDING_PURCHASE_CANCELED");

    private static final Map<String, SubscriptionState> VALUES = Arrays
        .stream(SubscriptionState.values())
        .collect(Collectors.toMap(ss -> ss.s, ss -> ss));

    private final String s;

    SubscriptionState(String s) {
      this.s = s;
    }

    private static Optional<SubscriptionState> fromString(String s) {
      return Optional.ofNullable(SubscriptionState.VALUES.getOrDefault(s, null));
    }

    @VisibleForTesting
    String apiString() {
      return s;
    }
  }

  // https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.productsv2#PurchaseStateContext
  @VisibleForTesting
  enum PurchaseState {
    PURCHASE_STATE_UNSPECIFIED("PURCHASE_STATE_UNSPECIFIED"),
    PURCHASED("PURCHASED"),
    CANCELLED("CANCELLED"),
    PENDING("PENDING");

    private static final Map<String, PurchaseState> VALUES = Arrays
        .stream(PurchaseState.values())
        .collect(Collectors.toMap(ss -> ss.s, ss -> ss));

    private final String s;

    PurchaseState(final String s) {
      this.s = s;
    }

    private static Optional<PurchaseState> fromString(String s) {
      return Optional.ofNullable(PurchaseState.VALUES.getOrDefault(s, null));
    }

    @VisibleForTesting
    String apiString() {
      return s;
    }
  }

  // https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.productsv2#ConsumptionState
  @VisibleForTesting
  enum ConsumptionState {
    UNSPECIFIED("CONSUMPTION_STATE_UNSPECIFIED"),
    YET_TO_BE_CONSUMED("CONSUMPTION_STATE_YET_TO_BE_CONSUMED"),
    CONSUMED("CONSUMPTION_STATE_CONSUMED");

    private static final Map<String, ConsumptionState> VALUES = Arrays
        .stream(ConsumptionState.values())
        .collect(Collectors.toMap(ss -> ss.s, ss -> ss));

    private final String s;

    ConsumptionState(final String s) {
      this.s = s;
    }

    private static Optional<ConsumptionState> fromString(String s) {
      return Optional.ofNullable(ConsumptionState.VALUES.getOrDefault(s, null));
    }

    @VisibleForTesting
    String apiString() {
      return s;
    }
  }

  // https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.subscriptionsv2#AcknowledgementState
  @VisibleForTesting
  enum AcknowledgementState {
    UNSPECIFIED("ACKNOWLEDGEMENT_STATE_UNSPECIFIED"),
    PENDING("ACKNOWLEDGEMENT_STATE_PENDING"),
    ACKNOWLEDGED("ACKNOWLEDGEMENT_STATE_ACKNOWLEDGED");

    private static final Map<String, AcknowledgementState> VALUES = Arrays
        .stream(AcknowledgementState.values())
        .collect(Collectors.toMap(as -> as.s, ss -> ss));

    private final String s;

    AcknowledgementState(String s) {
      this.s = s;
    }

    private static Optional<AcknowledgementState> fromString(String s) {
      return Optional.ofNullable(AcknowledgementState.VALUES.getOrDefault(s, null));
    }

    @VisibleForTesting
    String apiString() {
      return s;
    }
  }
}
