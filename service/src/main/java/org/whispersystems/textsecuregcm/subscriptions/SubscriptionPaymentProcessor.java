/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import org.whispersystems.textsecuregcm.storage.PaymentTime;

import java.util.concurrent.CompletableFuture;

public interface SubscriptionPaymentProcessor {

  PaymentProvider getProvider();

  /**
   * A receipt of payment from a payment provider
   *
   * @param itemId      An identifier for the payment that should be unique within the payment provider. Note that this
   *                    must identify an actual individual charge, not the subscription as a whole.
   * @param paymentTime The time this payment was for
   * @param level       The level which this payment corresponds to
   */
  record ReceiptItem(String itemId, PaymentTime paymentTime, long level) {}

  /**
   * Retrieve a {@link ReceiptItem} for the subscriptionId stored in the subscriptions table
   *
   * @param subscriptionId A subscriptionId that potentially corresponds to a valid subscription
   * @return A {@link ReceiptItem} if the subscription is valid
   */
  CompletableFuture<ReceiptItem> getReceiptItem(String subscriptionId);

  /**
   * Cancel all active subscriptions for this key within the payment provider.
   *
   * @param key An identifier for the subscriber within the payment provider, corresponds to the customerId field in the
   *            subscriptions table
   * @return A stage that completes when all subscriptions associated with the key are cancelled
   */
  CompletableFuture<Void> cancelAllActiveSubscriptions(String key);

  CompletableFuture<SubscriptionInformation> getSubscriptionInformation(final String subscriptionId);
}
