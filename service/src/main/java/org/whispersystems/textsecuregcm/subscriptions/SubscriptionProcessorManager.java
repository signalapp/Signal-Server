/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SubscriptionProcessorManager {
  SubscriptionProcessor getProcessor();

  boolean supportsPaymentMethod(PaymentMethod paymentMethod);

  boolean supportsCurrency(String currency);

  Set<String> getSupportedCurrencies();

  CompletableFuture<PaymentDetails> getPaymentDetails(String paymentId);

  CompletableFuture<ProcessorCustomer> createCustomer(byte[] subscriberUser);

  CompletableFuture<String> createPaymentMethodSetupToken(String customerId);


  /**
   * @param customerId
   * @param paymentMethodToken    a processor-specific token necessary
   * @param currentSubscriptionId (nullable) an active subscription ID, in case it needs an explicit update
   * @return
   */
  CompletableFuture<Void> setDefaultPaymentMethodForCustomer(String customerId, String paymentMethodToken,
      @Nullable String currentSubscriptionId);

  CompletableFuture<Object> getSubscription(String subscriptionId);

  CompletableFuture<SubscriptionId> createSubscription(String customerId, String templateId, long level,
                                                       long lastSubscriptionCreatedAt);

  CompletableFuture<SubscriptionId> updateSubscription(
      Object subscription, String templateId, long level, String idempotencyKey);

  CompletableFuture<Long> getLevelForSubscription(Object subscription);

  CompletableFuture<Void> cancelAllActiveSubscriptions(String customerId);

  CompletableFuture<ReceiptItem> getReceiptItem(String subscriptionId);

  CompletableFuture<SubscriptionInformation> getSubscriptionInformation(Object subscription);

  record PaymentDetails(String id,
                        Map<String, String> customMetadata,
                        PaymentStatus status,
                        Instant created) {

  }

  enum PaymentStatus {
    SUCCEEDED,
    PROCESSING,
    FAILED,
    UNKNOWN,
  }

  enum SubscriptionStatus {
    /**
     * The subscription is in good standing and the most recent payment was successful.
     */
    ACTIVE("active"),

    /**
     * Payment failed when creating the subscription, or the subscription’s start date is in the future.
     */
    INCOMPLETE("incomplete"),

    /**
     * Payment on the latest renewal either failed or wasn't attempted.
     */
    PAST_DUE("past_due"),

    /**
     * The subscription has been canceled.
     */
    CANCELED("canceled"),

    /**
     * The latest renewal hasn't been paid but the subscription remains in place.
     */
    UNPAID("unpaid"),

    /**
     * The status from the downstream processor is unknown.
     */
    UNKNOWN("unknown");


    private final String apiValue;

    SubscriptionStatus(String apiValue) {
      this.apiValue = apiValue;
    }

    public static SubscriptionStatus forApiValue(String status) {
      return switch (status) {
        case "active" -> ACTIVE;
        case "canceled", "incomplete_expired" -> CANCELED;
        case "unpaid" -> UNPAID;
        case "past_due" -> PAST_DUE;
        case "incomplete" -> INCOMPLETE;

        case "trialing" -> {
          final Logger logger = LoggerFactory.getLogger(SubscriptionProcessorManager.class);
          logger.error("Subscription has status that should never happen: {}", status);

          yield UNKNOWN;
        }
        default -> {
          final Logger logger = LoggerFactory.getLogger(SubscriptionProcessorManager.class);
          logger.error("Subscription has unknown status: {}", status);

          yield UNKNOWN;
        }
      };
    }

    public String getApiValue() {
      return apiValue;
    }
  }


  record SubscriptionId(String id) {

  }

  record SubscriptionInformation(SubscriptionPrice price, long level, Instant billingCycleAnchor,
                                 Instant endOfCurrentPeriod, boolean active, boolean cancelAtPeriodEnd,
                                 SubscriptionStatus status,
                                 ChargeFailure chargeFailure) {

  }

  record SubscriptionPrice(String currency, BigDecimal amount) {

  }

  record ChargeFailure(String code, String message, @Nullable String outcomeNetworkStatus,
                       @Nullable String outcomeReason, @Nullable String outcomeType) {

  }

  record ReceiptItem(String itemId, Instant expiration, long level) {

  }

}
