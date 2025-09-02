/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.Customer;
import com.braintreegateway.CustomerGateway;
import com.google.cloud.pubsub.v1.Publisher;
import com.stripe.StripeClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.stripe.exception.ApiException;
import com.stripe.exception.StripeException;
import com.stripe.service.SubscriptionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;

class StripeManagerTest {

  private StripeClient stripeClient;
  private StripeManager stripeManager;
  private ExecutorService executor;

  @BeforeEach
  void setup() {
    this.executor = Executors.newSingleThreadExecutor();
    this.stripeClient = mock(StripeClient.class);
    this.stripeManager = new StripeManager(
        this.stripeClient,
        executor,
        "idempotencyKey".getBytes(StandardCharsets.UTF_8),
        "boost",
        Map.of(PaymentMethod.CARD, Set.of("usd")));
  }

  @AfterEach
  void teardown() throws InterruptedException {
    this.executor.shutdownNow();
    this.executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void paymentRequiresAction() throws StripeException {
    final SubscriptionService subscriptionService = mock(SubscriptionService.class);
    final ApiException stripeException = new ApiException("Payment intent requires action",
        UUID.randomUUID().toString(), "subscription_payment_intent_requires_action", 400, new Exception());

    when(subscriptionService.create(any(), any())).thenThrow(stripeException);
    when(stripeClient.subscriptions()).thenReturn(subscriptionService);
    assertThatExceptionOfType(SubscriptionException.PaymentRequiresAction.class).isThrownBy(() ->
        stripeManager.createSubscription("customerId", "priceId", 1, 0));
  }
}
