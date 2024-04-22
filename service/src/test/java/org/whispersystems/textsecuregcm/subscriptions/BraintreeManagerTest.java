/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.braintreegateway.BraintreeGateway;
import com.braintreegateway.Customer;
import com.braintreegateway.CustomerGateway;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import com.google.cloud.pubsub.v1.Publisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;

class BraintreeManagerTest {

  private BraintreeGateway braintreeGateway;
  private BraintreeManager braintreeManager;

  @BeforeEach
  void setup() {
    braintreeGateway = mock(BraintreeGateway.class);
    braintreeManager = new BraintreeManager(braintreeGateway,
        Map.of(PaymentMethod.CARD, Set.of("usd")),
        Map.of("usd", "usdMerchant"),
        mock(BraintreeGraphqlClient.class),
        mock(CurrencyConversionManager.class),
        mock(Publisher.class),
        Executors.newSingleThreadExecutor());
  }

  @Test
  void cancelAllActiveSubscriptions_nullDefaultPaymentMethod() {

    final Customer customer = mock(Customer.class);
    when(customer.getDefaultPaymentMethod()).thenReturn(null);

    final CustomerGateway customerGateway = mock(CustomerGateway.class);
    when(customerGateway.find(anyString())).thenReturn(customer);

    when(braintreeGateway.customer()).thenReturn(customerGateway);

    assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
        braintreeManager.cancelAllActiveSubscriptions("customerId")).join();
  }
}
