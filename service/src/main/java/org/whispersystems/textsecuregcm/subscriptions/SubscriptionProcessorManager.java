/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriptionProcessorManager {

  SubscriptionProcessor getProcessor();

  boolean supportsPaymentMethod(PaymentMethod paymentMethod);

  CompletableFuture<ProcessorCustomer> createCustomer(byte[] subscriberUser);

  CompletableFuture<String> createPaymentMethodSetupToken(String customerId);

  Set<String> getSupportedCurrencies();
}
