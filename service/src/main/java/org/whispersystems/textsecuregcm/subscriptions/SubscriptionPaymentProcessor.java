/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.PaymentTime;

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
   * @throws RateLimitExceededException                          If rate-limited
   * @throws SubscriptionNotFoundException                       If the provided subscriptionId could not be found with
   *                                                             the provider
   * @throws SubscriptionPaymentRequiredException                If the subscription is in a state does not grant the
   *                                                             user an entitlement
   * @throws SubscriptionReceiptRequestedForOpenPaymentException If a receipt was requested while a payment transaction
   *                                                             was still open
   */
  ReceiptItem getReceiptItem(String subscriptionId)
      throws RateLimitExceededException, SubscriptionNotFoundException, SubscriptionChargeFailurePaymentRequiredException, SubscriptionPaymentRequiredException, SubscriptionReceiptRequestedForOpenPaymentException;

  /**
   * Cancel all active subscriptions for this key within the payment processor.
   *
   * @param key An identifier for the subscriber within the payment provider, corresponds to the customerId field in the
   *            subscriptions table
   * @throws RateLimitExceededException             If rate-limited
   * @throws SubscriptionInvalidArgumentsException If a precondition for cancellation was not met
   */
  void cancelAllActiveSubscriptions(String key)
      throws SubscriptionInvalidArgumentsException, RateLimitExceededException;

  /**
   * Retrieve subscription information from the processor
   *
   * @param subscriptionId The identifier with the processor to retrieve information for
   * @return {@link SubscriptionInformation} from the provider
   * @throws RateLimitExceededException    If rate-limited
   * @throws SubscriptionNotFoundException If the provided key was not found with the provider
   */
  SubscriptionInformation getSubscriptionInformation(final String subscriptionId)
      throws RateLimitExceededException, SubscriptionNotFoundException;
}
