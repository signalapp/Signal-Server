/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import com.google.api.services.androidpublisher.model.Money;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Utility for converting currency amounts between Google, Stripe, Braintree, configuration, and this server's API
/// responses. Generally, amounts are either represented as:
///
/// - Arbitrary-precision decimal ([BigDecimal]) in the currency's primary unit (e.g. dollars for USD, yen for JPY)
/// - Integral numbers ([Long]) in the currency's minor unit (e.g. cents for USD, yen for JPY)
///
/// There is no ISO standard for the definition of currency minor units. Unless otherwise noted, amounts in currency's
/// minor unit follow [Stripe's](https://docs.stripe.com/currencies?presentment-currency=US#zero-decimal)
/// interpretation.
///
/// Unless otherwise noted, the server's external APIs prefer minor units.
public class SubscriptionCurrencyUtil {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionCurrencyUtil.class);

  // This list was taken from https://stripe.com/docs/currencies?presentment-currency=US
  private static final Set<String> stripeZeroDecimalCurrencies = Set.of("bif", "clp", "djf", "gnf", "jpy", "kmf", "krw",
      "mga", "pyg", "rwf", "vnd", "vuv", "xaf", "xof", "xpf");


  /// Takes an amount in a currency's primary unit (e.g. `USD 4.99`, `JPY 500`) and returns it in the currency's minor
  /// unit (`499`, `500`)
  ///
  /// @param currency             The amount's currency
  /// @param amountInPrimaryUnits The amount in the currency's primary unit
  ///
  /// @return The amount in the currency's minor units (as defined by stripe)
  /// @throws IllegalArgumentException If the amount cannot be represented as a whole number in the currency's minor
  ///                                  units
  public static long convertPrimaryToMinorUnits(String currency, BigDecimal amountInPrimaryUnits) {
    final BigDecimal amountInMinorUnits = scaleToMinorUnits(currency, amountInPrimaryUnits);

    if (amountInMinorUnits.stripTrailingZeros().scale() > 0) {
      throw new IllegalArgumentException(String.format("%s %s is not a whole number in minor units of the currency",
          currency, amountInPrimaryUnits));
    }

    try {
      return amountInMinorUnits.longValueExact();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(String.format("%s %s is too large to represent in minor units of the currency",
          currency, amountInPrimaryUnits));
    }
  }

  /// Takes an amount in a currency's minor unit (e.g. `USD 499`, `JPY 500``) and returns it in the currency's primary
  /// unit (`4.99`, `500`)
  ///
  /// @param currency           The amount's currency
  /// @param amountInMinorUnits The amount in the currency's minor unit
  /// @return The amount in the currency's primary units
  public static BigDecimal convertMinorToPrimaryUnits(String currency, long amountInMinorUnits) {
    if (stripeZeroDecimalCurrencies.contains(currency.toLowerCase(Locale.ROOT))) {
      return BigDecimal.valueOf(amountInMinorUnits);
    }
    return BigDecimal.valueOf(amountInMinorUnits).scaleByPowerOfTen(-2);
  }

  /// Convert from Braintree's API representation (primary units) to minor units
  static long convertBraintreeAmountToMinorUnits(final String currency, final BigDecimal amount) {
    return convertPrimaryToMinorUnits(currency, amount);
  }

  /// Convert Play Billing's representation of currency amounts to minor units
  ///
  /// @see SubscriptionCurrencyUtil
  static long convertGoogleMoneyToMinorUnits(final Money money) {
    final BigDecimal fractionalComponent = money.getNanos() == null
        ? BigDecimal.ZERO
        : BigDecimal.valueOf(money.getNanos()).scaleByPowerOfTen(-9);
    final BigDecimal amount = BigDecimal.valueOf(money.getUnits()).add(fractionalComponent);
    return roundToMinorUnits("google", money.getCurrencyCode(), amount);
  }

  /// Convert the App Store's representation of a price to minor units. The App Store reports prices in milliunits of
  /// the currency's primary unit, so `USD 4.99` is reported as `4990`.
  ///
  /// @param currency          The price's currency
  /// @param priceInMilliUnits The price in thousandths of the currency's primary unit
  /// @see SubscriptionCurrencyUtil#roundToMinorUnits
  static long convertAppleMoneyToMinorUnits(final String currency, final long priceInMilliUnits) {
    return roundToMinorUnits("apple", currency, BigDecimal.valueOf(priceInMilliUnits).scaleByPowerOfTen(-3));
  }

  private static BigDecimal scaleToMinorUnits(final String currency, final BigDecimal amountInPrimaryUnits) {
    if (stripeZeroDecimalCurrencies.contains(currency.toLowerCase(Locale.ROOT))) {
      return amountInPrimaryUnits;
    }
    return amountInPrimaryUnits.scaleByPowerOfTen(2);
  }

  private static long roundToMinorUnits(final String provider, final String currency, final BigDecimal amountInPrimaryUnits) {
    final BigDecimal amountInMinorUnits = scaleToMinorUnits(currency, amountInPrimaryUnits);

    if (amountInMinorUnits.stripTrailingZeros().scale() > 0) {
      logger.warn("Payment provider {} reported {} {}, which is not a whole number of minor units; rounding",
          provider, currency, amountInPrimaryUnits);
    }

    return amountInMinorUnits.setScale(0, RoundingMode.HALF_UP).longValueExact();
  }
}
