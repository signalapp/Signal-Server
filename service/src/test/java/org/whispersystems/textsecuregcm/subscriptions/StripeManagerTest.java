/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.stripe.StripeClient;
import com.stripe.exception.ApiException;
import com.stripe.exception.StripeException;
import com.stripe.model.Price;
import com.stripe.model.StripeCollection;
import com.stripe.model.Subscription;
import com.stripe.model.SubscriptionItem;
import com.stripe.param.SubscriptionUpdateParams;
import com.stripe.service.SubscriptionItemService;
import com.stripe.service.SubscriptionService;
import com.stripe.service.V1Services;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StripeManagerTest {

  private StripeClient stripeClient;
  private SubscriptionService subscriptionService;
  private SubscriptionItemService subscriptionItemsService;
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

    final V1Services v1Services = mock(V1Services.class);
    when(stripeClient.v1()).thenReturn(v1Services);

    subscriptionService = mock(SubscriptionService.class);
    when(v1Services.subscriptions()).thenReturn(subscriptionService);
    subscriptionItemsService = mock(SubscriptionItemService.class);
    when(v1Services.subscriptionItems()).thenReturn(subscriptionItemsService);
  }

  @AfterEach
  void teardown() throws InterruptedException {
    this.executor.shutdownNow();
    this.executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void paymentRequiresAction() throws StripeException {
    final ApiException stripeException = new ApiException("Payment intent requires action",
        UUID.randomUUID().toString(), "subscription_payment_intent_requires_action", 400, new Exception());

    when(subscriptionService.create(any(), any())).thenThrow(stripeException);
    assertThatExceptionOfType(SubscriptionPaymentRequiresActionException.class).isThrownBy(() ->
        stripeManager.createSubscription("customerId", "priceId", 1, 0));
  }

  @ParameterizedTest
  @CsvSource(
      {
          "usd, unpaid, true",
          "usd, past_due, true",
          "usd, incomplete, true",
          "usd, active, false",
          "zzz, active, true",
      }
  )
  void testEndSubscription(final String currency, final String status, final boolean expectCancelImmediately) throws Exception {
    final Subscription subscription = mock(Subscription.class);
    when(subscription.getId()).thenReturn("test-subscription");
    when(subscription.getStatus()).thenReturn(status);

    final SubscriptionItem item = mock(SubscriptionItem.class);
    final Price price = new Price();
    price.setCurrency(currency);
    when(item.getPrice()).thenReturn(price);

    @SuppressWarnings("unchecked") final StripeCollection<SubscriptionItem> items = mock(StripeCollection.class);
    when(items.autoPagingIterable()).thenReturn(List.of(item));
    when(subscriptionItemsService.list(any(), any()))
        .thenReturn(items);

    stripeManager.endSubscription(subscription);

    verify(subscriptionService, expectCancelImmediately ? times(1) : never())
        .cancel(any(), any(), any());
    verify(subscriptionService, expectCancelImmediately ? never() : times(1))
        .update(any(), assertArg(
            (Consumer<SubscriptionUpdateParams>) params -> assertTrue(params.getCancelAtPeriodEnd())),
            any());

  }

}
