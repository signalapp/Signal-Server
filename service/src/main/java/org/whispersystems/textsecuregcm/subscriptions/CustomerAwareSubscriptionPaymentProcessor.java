/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

/**
 * Interface for an external payment provider that has an API-accessible notion of customer that implementations can
 * manage. Payment providers that let you add and remove payment methods to an existing customer should implement this
 * interface. Contrast this with the super interface {@link SubscriptionPaymentProcessor}, which allows for a payment
 * provider with an API that only operations on subscriptions.
 */
public interface CustomerAwareSubscriptionPaymentProcessor extends SubscriptionPaymentProcessor {

  boolean supportsPaymentMethod(PaymentMethod paymentMethod);

  Set<String> getSupportedCurrenciesForPaymentMethod(PaymentMethod paymentMethod);

  CompletableFuture<ProcessorCustomer> createCustomer(byte[] subscriberUser, @Nullable ClientPlatform clientPlatform);

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

  /**
   * @param subscription
   * @return the subscription’s current level and lower-case currency code
   */
  CompletableFuture<LevelAndCurrency> getLevelAndCurrencyForSubscription(Object subscription);

  record SubscriptionId(String id) {

  }

  record LevelAndCurrency(long level, String currency) {

  }

}
