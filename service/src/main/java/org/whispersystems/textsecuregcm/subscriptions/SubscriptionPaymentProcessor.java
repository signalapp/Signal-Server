/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.PaymentTime;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;

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
   *
   * @throws RateLimitExceededException                           If rate-limited
   * @throws SubscriptionException.NotFound                       If the provided subscriptionId could not be found with
   *                                                              the provider
   * @throws SubscriptionException.InvalidArguments               If the subscriptionId locates a subscription that
   *                                                              cannot be used to generate a receipt
   * @throws SubscriptionException.PaymentRequired                If the subscription is in a state does not grant the
   *                                                              user an entitlement
   * @throws SubscriptionException.ChargeFailurePaymentRequired   If the subscription is in a state does not grant the
   *                                                              user an entitlement because a charge failed to go
   *                                                              through
   * @throws SubscriptionException.ReceiptRequestedForOpenPayment If a receipt was requested while a payment transaction
   *                                                              was still open
   */
  ReceiptItem getReceiptItem(String subscriptionId)
      throws SubscriptionException.InvalidArguments, RateLimitExceededException, SubscriptionException.NotFound, SubscriptionException.ChargeFailurePaymentRequired, SubscriptionException.PaymentRequired, SubscriptionException.ReceiptRequestedForOpenPayment;

  /**
   * Cancel all active subscriptions for this key within the payment processor.
   *
   * @param key An identifier for the subscriber within the payment provider, corresponds to the customerId field in the
   *            subscriptions table
   * @throws RateLimitExceededException             If rate-limited
   * @throws SubscriptionException.NotFound         If the provided key was not found with the provider
   * @throws SubscriptionException.InvalidArguments If a precondition for cancellation was not met
   */
  void cancelAllActiveSubscriptions(String key)
      throws SubscriptionException.InvalidArguments, RateLimitExceededException, SubscriptionException.NotFound;

  /**
   * Retrieve subscription information from the processor
   *
   * @param subscriptionId The identifier with the processor to retrieve information for
   * @return {@link SubscriptionInformation} from the provider
   * @throws RateLimitExceededException             If rate-limited
   * @throws SubscriptionException.NotFound         If the provided key was not found with the provider
   * @throws SubscriptionException.InvalidArguments If the subscription exists on the provider but does not represent a
   *                                                valid subscription
   */
  SubscriptionInformation getSubscriptionInformation(final String subscriptionId)
      throws SubscriptionException.InvalidArguments, RateLimitExceededException, SubscriptionException.NotFound;
}
