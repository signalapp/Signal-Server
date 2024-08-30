/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.google.api.services.androidpublisher.model.Money;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Set;

/**
 * Utility for scaling amounts among Stripe, Braintree, configuration, and API responses.
 * <p>
 * In general, the API input and output follow’s Stripe’s <a href= >specification</a> to use amounts in a currency’s
 * smallest unit. The exception is configuration APIs, which return values in the currency’s primary unit. Braintree
 * uses the currency’s primary unit for its input and output.
 * <h2>Examples</h2>
 * <table>
 *   <thead>
 *     <td>Currency, Amount</td>API</td><td>Stripe</td><td>Braintree</td>
 *   </thead>
 *   <tbody>
 *   <tr>
 *     <td>USD 4.99</td><td>499</td><td>499</td><td>4.99</td>
 *   </tr>
 *   <tr>
 *     <td>JPY 501</td><td>501</td><td>501</td><td>501</td>
 *   </tr>
 *   </tbody>
 * </table>
 */
public class SubscriptionCurrencyUtil {

  // This list was taken from https://stripe.com/docs/currencies?presentment-currency=US
  // Braintree
  private static final Set<String> stripeZeroDecimalCurrencies = Set.of("bif", "clp", "djf", "gnf", "jpy", "kmf", "krw",
      "mga", "pyg", "rwf", "vnd", "vuv", "xaf", "xof", "xpf");


  /**
   * Takes an amount as configured and turns it into an amount as API clients (and Stripe) expect to see it. For
   * instance, {@code USD 4.99} return {@code 499}, while {@code JPY 500} returns {@code 500}.
   *
   * <p>
   * Stripe appears to only support zero- and two-decimal currencies, but also has some backwards compatibility issues
   * with 0 decimal currencies, so this is not to any ISO standard but rather directly from Stripe's API doc page.
   */
  public static BigDecimal convertConfiguredAmountToApiAmount(String currency, BigDecimal configuredAmount) {
    if (stripeZeroDecimalCurrencies.contains(currency.toLowerCase(Locale.ROOT))) {
      return configuredAmount;
    }

    return configuredAmount.scaleByPowerOfTen(2);
  }

  /**
   * @see org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil#convertConfiguredAmountToApiAmount(String,
   * BigDecimal)
   */
  public static BigDecimal convertConfiguredAmountToStripeAmount(String currency, BigDecimal configuredAmount) {
    return convertConfiguredAmountToApiAmount(currency, configuredAmount);
  }

  /**
   * Braintree’s API expects amounts in a currency’s primary unit (e.g. USD 4.99)
   *
   * @see org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil#convertConfiguredAmountToApiAmount(String,
   * BigDecimal)
   */
  static BigDecimal convertBraintreeAmountToApiAmount(final String currency, final BigDecimal amount) {
    return convertConfiguredAmountToApiAmount(currency, amount);
  }

  /**
   * Convert Play Billing's representation of currency amounts to a Stripe-style amount
   *
   * @see org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil#convertConfiguredAmountToApiAmount(String,
   * BigDecimal)
   */
  static BigDecimal convertGoogleMoneyToApiAmount(final Money money) {
    final BigDecimal fractionalComponent = BigDecimal.valueOf(money.getNanos()).scaleByPowerOfTen(-9);
    final BigDecimal amount = BigDecimal.valueOf(money.getUnits()).add(fractionalComponent);
    return convertConfiguredAmountToApiAmount(money.getCurrencyCode(), amount);
  }
}
