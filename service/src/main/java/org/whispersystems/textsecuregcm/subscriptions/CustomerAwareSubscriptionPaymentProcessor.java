/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;
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

  /**
   * Create a customer on the payment processor
   *
   * @param subscriberUser An identifier that will be stored with the customer
   * @param clientPlatform The {@link ClientPlatform} of the requesting client
   * @return A {@link ProcessorCustomer} that can be used to identify this customer on the provider
   */
  ProcessorCustomer createCustomer(byte[] subscriberUser, @Nullable ClientPlatform clientPlatform);

  String createPaymentMethodSetupToken(String customerId);


  /**
   * Set a default payment method
   *
   * @param customerId            The customer to add a default payment method to
   * @param paymentMethodToken    a processor-specific token previously acquired at
   *                              {@link #createPaymentMethodSetupToken}
   * @param currentSubscriptionId (nullable) an active subscription ID, in case it needs an explicit update
   * @throws SubscriptionException.InvalidArguments If the paymentMethodToken is invalid or the payment method has not
   *                                                finished being set up
   */
  void setDefaultPaymentMethodForCustomer(String customerId, String paymentMethodToken,
      @Nullable String currentSubscriptionId) throws SubscriptionException.InvalidArguments;

  Object getSubscription(String subscriptionId);

  /**
   * Create a subscription on a customer
   *
   * @param customerId                The customer to create the subscription on
   * @param templateId                An identifier for the type of subscription to create
   * @param level                     The level of the subscription
   * @param lastSubscriptionCreatedAt The timestamp of the last successfully created subscription
   * @return A subscription identifier
   * @throws SubscriptionException.ProcessorException If there was a failure processing the charge
   * @throws SubscriptionException.InvalidArguments   If there was a failure because an idempotency key was reused on a
   *                                                  modified request, or if the payment requires additional steps
   *                                                  before charging
   * @throws SubscriptionException.ProcessorConflict  If there was no payment method on the customer
   */
  SubscriptionId createSubscription(String customerId, String templateId, long level, long lastSubscriptionCreatedAt)
      throws SubscriptionException.ProcessorException, SubscriptionException.InvalidArguments, SubscriptionException.ProcessorConflict;

  /**
   * Update an existing subscription on a customer
   *
   * @param subscription              The subscription to update
   * @param templateId                An identifier for the new subscription type
   * @param level                     The target level of the subscription
   * @param idempotencyKey            An idempotency key to prevent retries of successful requests
   * @return A subscription identifier
   * @throws SubscriptionException.ProcessorException If there was a failure processing the charge
   * @throws SubscriptionException.InvalidArguments   If there was a failure because an idempotency key was reused on a
   *                                                  modified request, or if the payment requires additional steps
   *                                                  before charging
   * @throws SubscriptionException.ProcessorConflict  If there was no payment method on the customer
   */
  SubscriptionId updateSubscription(Object subscription, String templateId, long level, String idempotencyKey)
      throws SubscriptionException.InvalidArguments, SubscriptionException.ProcessorException, SubscriptionException.ProcessorConflict;

  /**
   * @param subscription
   * @return the subscription’s current level and lower-case currency code
   */
  LevelAndCurrency getLevelAndCurrencyForSubscription(Object subscription);

  record SubscriptionId(String id) {

  }

  record LevelAndCurrency(long level, String currency) {

  }

}
