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
import com.google.api.services.androidpublisher.model.BasePlan;
import com.google.api.services.androidpublisher.model.OfferDetails;
import com.google.api.services.androidpublisher.model.RegionalBasePlanConfig;
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
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

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
public class GooglePlayBillingManager implements SubscriptionPaymentProcessor {

  private static final Logger logger = LoggerFactory.getLogger(GooglePlayBillingManager.class);

  private final AndroidPublisher androidPublisher;
  private final Executor executor;
  private final String packageName;
  private final Map<String, Long> productIdToLevel;
  private final Clock clock;

  private static final String VALIDATE_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "validate");
  private static final String CANCEL_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "cancel");
  private static final String GET_RECEIPT_COUNTER_NAME = MetricsUtil.name(GooglePlayBillingManager.class, "getReceipt");


  public GooglePlayBillingManager(
      final InputStream credentialsStream,
      final String packageName,
      final String applicationName,
      final Map<String, Long> productIdToLevel,
      final Executor executor)
      throws GeneralSecurityException, IOException {
    this(new AndroidPublisher.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(GoogleCredentials
                .fromStream(credentialsStream)
                .createScoped(AndroidPublisherScopes.ANDROIDPUBLISHER)))
            .setApplicationName(applicationName)
            .build(),
        Clock.systemUTC(), packageName, productIdToLevel, executor);
  }

  @VisibleForTesting
  GooglePlayBillingManager(
      final AndroidPublisher androidPublisher,
      final Clock clock,
      final String packageName,
      final Map<String, Long> productIdToLevel,
      final Executor executor) {
    this.clock = clock;
    this.androidPublisher = androidPublisher;
    this.productIdToLevel = productIdToLevel;
    this.executor = Objects.requireNonNull(executor);
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
     * @return A stage that completes when the purchase has been successfully acknowledged
     */
    public CompletableFuture<Void> acknowledgePurchase() {
      if (!requiresAck) {
        // We've already acknowledged this purchase on a previous attempt, nothing to do
        return CompletableFuture.completedFuture(null);
      }
      return executeTokenOperation(pub -> pub.purchases().subscriptions()
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
   * @return A stage that completes successfully when the token has been validated, or fails if the token does not
   * represent an active purchase
   */
  public CompletableFuture<ValidatedToken> validateToken(String purchaseToken) {
    return lookupSubscription(purchaseToken).thenApplyAsync(subscription -> {

      final SubscriptionState state = SubscriptionState
          .fromString(subscription.getSubscriptionState())
          .orElse(SubscriptionState.UNSPECIFIED);

      Metrics.counter(VALIDATE_COUNTER_NAME, subscriptionTags(subscription)).increment();

      // We only ever acknowledge valid tokens. There are cases where a subscription was once valid and then was
      // cancelled, so the user could still be entitled to their purchase. However, if we never acknowledge it, the
      // user's charge will eventually be refunded anyway. See
      // https://developer.android.com/google/play/billing/integrate#pending
      if (state != SubscriptionState.ACTIVE) {
        throw ExceptionUtils.wrap(new SubscriptionException.PaymentRequired(
            "Cannot acknowledge purchase for subscription in state " + subscription.getSubscriptionState()));
      }

      final AcknowledgementState acknowledgementState = AcknowledgementState
          .fromString(subscription.getAcknowledgementState())
          .orElse(AcknowledgementState.UNSPECIFIED);

      final boolean requiresAck = switch (acknowledgementState) {
        case ACKNOWLEDGED -> false;
        case PENDING -> true;
        case UNSPECIFIED -> throw ExceptionUtils.wrap(
            new IOException("Invalid acknowledgement state " + subscription.getAcknowledgementState()));
      };

      final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);
      final long level = productIdToLevel(purchase.getProductId());

      return new ValidatedToken(level, purchase.getProductId(), purchaseToken, requiresAck);
    }, executor);
  }


  /**
   * Cancel the subscription. Cancellation stops auto-renewal, but does not refund the user nor cut off access to their
   * entitlement until their current period expires.
   *
   * @param purchaseToken The purchaseToken associated with the subscription
   * @return A stage that completes when the subscription has successfully been cancelled
   */
  public CompletableFuture<Void> cancelAllActiveSubscriptions(String purchaseToken) {
    return lookupSubscription(purchaseToken).thenCompose(subscription -> {
      Metrics.counter(CANCEL_COUNTER_NAME, subscriptionTags(subscription)).increment();

      final SubscriptionState state = SubscriptionState
          .fromString(subscription.getSubscriptionState())
          .orElse(SubscriptionState.UNSPECIFIED);

      if (state == SubscriptionState.CANCELED || state == SubscriptionState.EXPIRED) {
        // already cancelled, nothing to do
        return CompletableFuture.completedFuture(null);
      }
      final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);

      return executeTokenOperation(pub ->
          pub.purchases().subscriptions().cancel(packageName, purchase.getProductId(), purchaseToken));
    })
    // If the subscription is not found, no need to do anything
    .exceptionally(ExceptionUtils.exceptionallyHandler(SubscriptionException.NotFound.class, e -> null));
  }

  @Override
  public CompletableFuture<SubscriptionInformation> getSubscriptionInformation(final String purchaseToken) {

    final CompletableFuture<SubscriptionPurchaseV2> subscriptionFuture = lookupSubscription(purchaseToken);
    final CompletableFuture<SubscriptionPrice> priceFuture = subscriptionFuture.thenCompose(this::getSubscriptionPrice);

    return subscriptionFuture.thenCombineAsync(priceFuture, (subscription, price) -> {

      final SubscriptionPurchaseLineItem lineItem = getLineItem(subscription);
      final Optional<Instant> billingCycleAnchor = getStartTime(subscription);
      final Optional<Instant> expiration = getExpiration(lineItem);

      final SubscriptionStatus status = switch (SubscriptionState
          .fromString(subscription.getSubscriptionState())
          .orElse(SubscriptionState.UNSPECIFIED)) {
        case ACTIVE -> SubscriptionStatus.ACTIVE;
        case PENDING -> SubscriptionStatus.INCOMPLETE;
        case EXPIRED, ON_HOLD, PAUSED -> SubscriptionStatus.PAST_DUE;
        case IN_GRACE_PERIOD -> SubscriptionStatus.UNPAID;
        case CANCELED, PENDING_PURCHASE_CANCELED -> SubscriptionStatus.CANCELED;
        case UNSPECIFIED -> SubscriptionStatus.UNKNOWN;
      };

      final boolean autoRenewEnabled = Optional
          .ofNullable(lineItem.getAutoRenewingPlan())
          .map(AutoRenewingPlan::getAutoRenewEnabled) // returns null or false if auto-renew disabled
          .orElse(false);
      return new SubscriptionInformation(
          price,
          productIdToLevel(lineItem.getProductId()),
          billingCycleAnchor.orElse(null),
          expiration.orElse(null),
          expiration.map(clock.instant()::isBefore).orElse(false),
          !autoRenewEnabled,
          status,
          PaymentProvider.GOOGLE_PLAY_BILLING,
          PaymentMethod.GOOGLE_PLAY_BILLING,
          false,
          null);
    }, executor);
  }

  private CompletableFuture<SubscriptionPrice> getSubscriptionPrice(final SubscriptionPurchaseV2 subscriptionPurchase) {

    final SubscriptionPurchaseLineItem lineItem = getLineItem(subscriptionPurchase);
    final OfferDetails offerDetails = lineItem.getOfferDetails();
    final String basePlanId = offerDetails.getBasePlanId();

    return this.executeAsync(pub -> pub.monetization().subscriptions().get(packageName, lineItem.getProductId()))
        .thenApplyAsync(subscription -> {

          final BasePlan basePlan = subscription.getBasePlans().stream()
              .filter(bp -> bp.getBasePlanId().equals(basePlanId))
              .findFirst()
              .orElseThrow(() -> ExceptionUtils.wrap(new IOException("unknown basePlanId " + basePlanId)));
          final String region = subscriptionPurchase.getRegionCode();
          final RegionalBasePlanConfig basePlanConfig = basePlan.getRegionalConfigs()
              .stream()
              .filter(rbpc -> Objects.equals(region, rbpc.getRegionCode()))
              .findFirst()
              .orElseThrow(() -> ExceptionUtils.wrap(new IOException("unknown subscription region " + region)));

          return new SubscriptionPrice(
              basePlanConfig.getPrice().getCurrencyCode().toUpperCase(Locale.ROOT),
              SubscriptionCurrencyUtil.convertGoogleMoneyToApiAmount(basePlanConfig.getPrice()));
        }, executor);
  }

  @Override
  public CompletableFuture<ReceiptItem> getReceiptItem(String purchaseToken) {
    return lookupSubscription(purchaseToken).thenApplyAsync(subscription -> {
      final AcknowledgementState acknowledgementState = AcknowledgementState
          .fromString(subscription.getAcknowledgementState())
          .orElse(AcknowledgementState.UNSPECIFIED);
      if (acknowledgementState != AcknowledgementState.ACKNOWLEDGED) {
        // We should only ever generate receipts for a stored and acknowledged token.
        logger.error("Tried to fetch receipt for purchaseToken {} that was never acknowledged", purchaseToken);
        throw new IllegalStateException("Tried to fetch receipt for purchaseToken that was never acknowledged");
      }

      Metrics.counter(GET_RECEIPT_COUNTER_NAME, subscriptionTags(subscription)).increment();

      final SubscriptionPurchaseLineItem purchase = getLineItem(subscription);
      final Instant expiration = getExpiration(purchase)
          .orElseThrow(() -> ExceptionUtils.wrap(new IOException("Invalid subscription expiration")));

      if (expiration.isBefore(clock.instant())) {
        // We don't need to check any state at this point, just whether the subscription is currently valid. If the
        // subscription is in a grace period, the expiration time will be dynamically extended, see
        // https://developer.android.com/google/play/billing/lifecycle/subscriptions#grace-period
        throw ExceptionUtils.wrap(new SubscriptionException.PaymentRequired());
      }

      return new ReceiptItem(
          subscription.getLatestOrderId(),
          PaymentTime.periodEnds(expiration),
          productIdToLevel(purchase.getProductId()));
    }, executor);
  }


  interface ApiCall<T> {

    AndroidPublisherRequest<T> req(AndroidPublisher publisher) throws IOException;
  }

  /**
   * Asynchronously execute a synchronous API call from an AndroidPublisher
   *
   * @param apiCall A function that takes the publisher and returns the API call to execute
   * @param <R>     The return type of the executed ApiCall
   * @return A stage that completes with the result of the API call
   */
  private <R> CompletableFuture<R> executeAsync(final ApiCall<R> apiCall) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return apiCall.req(androidPublisher).execute();
      } catch (IOException e) {
        throw ExceptionUtils.wrap(e);
      }
    }, executor);
  }

  /**
   * Asynchronously execute a synchronous API call on a purchaseToken, mapping expected errors to the appropriate
   * {@link SubscriptionException}
   *
   * @param apiCall An API call that operates on a purchaseToken
   * @param <R>     The result of the API call
   * @return A stage that completes with the result of the API call
   */
  private <R> CompletableFuture<R> executeTokenOperation(final ApiCall<R> apiCall) {
    return executeAsync(apiCall)
        .exceptionally(ExceptionUtils.exceptionallyHandler(HttpResponseException.class, e -> {
          if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()
              || e.getStatusCode() == Response.Status.GONE.getStatusCode()) {
            throw ExceptionUtils.wrap(new SubscriptionException.NotFound());
          }
          final String details = e instanceof GoogleJsonResponseException
              ? ((GoogleJsonResponseException) e).getDetails().toString()
              : "";
          logger.warn("Unexpected HTTP status code {} from androidpublisher: {}", e.getStatusCode(), details, e);
          throw ExceptionUtils.wrap(e);
        }));
  }

  private CompletableFuture<SubscriptionPurchaseV2> lookupSubscription(final String purchaseToken) {
    return executeTokenOperation(publisher -> publisher.purchases().subscriptionsv2().get(packageName, purchaseToken));
  }

  private long productIdToLevel(final String productId) {
    final Long level = this.productIdToLevel.get(productId);
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
