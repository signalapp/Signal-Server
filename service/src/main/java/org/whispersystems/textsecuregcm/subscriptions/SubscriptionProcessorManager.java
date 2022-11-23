/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriptionProcessorManager {

  SubscriptionProcessor getProcessor();

  boolean supportsPaymentMethod(PaymentMethod paymentMethod);

  boolean supportsCurrency(String currency);

  Set<String> getSupportedCurrencies();

  CompletableFuture<PaymentDetails> getPaymentDetails(String paymentId);

  CompletableFuture<ProcessorCustomer> createCustomer(byte[] subscriberUser);

  CompletableFuture<String> createPaymentMethodSetupToken(String customerId);

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
}
