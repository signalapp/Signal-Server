/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.api.services.androidpublisher.model.Money;
import java.math.BigDecimal;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SubscriptionCurrencyUtilTest {

  @ParameterizedTest
  @MethodSource
  void convertConfiguredAmountToApiAmount(final String currency, final String amount, final long expected) {
    assertThat(SubscriptionCurrencyUtil.convertPrimaryToMinorUnits(currency, new BigDecimal(amount)))
        .isEqualTo(expected);
  }

  static Stream<Arguments> convertConfiguredAmountToApiAmount() {
    return Stream.of(
        Arguments.of("usd", "5", 500L),
        Arguments.of("usd", "4.99", 499L),
        Arguments.of("USD", "4.99", 499L),
        Arguments.of("jpy", "500", 500L),
        Arguments.of("JPY", "500", 500L));
  }


  @ParameterizedTest
  @MethodSource
  void convertMinorToPrimaryUnits(final String currency, final long amountInMinorUnits, final String expected) {
    assertThat(SubscriptionCurrencyUtil.convertMinorToPrimaryUnits(currency, amountInMinorUnits))
        .hasToString(expected);
  }

  static Stream<Arguments> convertMinorToPrimaryUnits() {
    return Stream.of(
        Arguments.of("usd", 499L, "4.99"),
        Arguments.of("usd", 250L, "2.50"),
        Arguments.of("usd", 500L, "5.00"),
        Arguments.of("eur", 1000000L, "10000.00"),
        Arguments.of("jpy", 500L, "500"),
        Arguments.of("bif", 2500L, "2500"),
        Arguments.of("USD", 499L, "4.99"));
  }

  @ParameterizedTest
  @MethodSource
  void convertGoogleMoneyToApiAmount(final String currency, final long units, @Nullable final Integer nanos, final long expected) {
    final Money money = new Money();
    money.setCurrencyCode(currency);
    money.setUnits(units);
    money.setNanos(nanos);

    assertThat(SubscriptionCurrencyUtil.convertGoogleMoneyToMinorUnits(money)).isEqualTo(expected);
  }

  static Stream<Arguments> convertGoogleMoneyToApiAmount() {
    return Stream.of(
        Arguments.argumentSet("usd", "USD", 4L, 990000000, 499L),
        Arguments.argumentSet("usd-only-nanos", "USD", 0L, 500000000, 50L),
        Arguments.argumentSet("usd-null-nanos","USD", 4L, null, 400L),
        Arguments.argumentSet("jpy", "JPY", 500L, 0, 500L),
        Arguments.argumentSet("usd-round-up", "USD", 1L, 995000000, 200L),
        Arguments.argumentSet("usd-round-down", "USD", 1L, 994000000, 199L));
  }

  @ParameterizedTest
  @MethodSource
  void convertAppleMoneyToApiAmount(final String currency, final long priceInMilliUnits, final long expected) {
    assertThat(SubscriptionCurrencyUtil.convertAppleMoneyToMinorUnits(currency, priceInMilliUnits))
        .isEqualTo(expected);
  }

  static Stream<Arguments> convertAppleMoneyToApiAmount() {
    return Stream.of(
        Arguments.of("usd", 4990L, 499L),
        Arguments.of("usd", 1500L, 150L),
        Arguments.of("usd", 1000L, 100L),
        Arguments.of("jpy", 500000L, 500L),
        // Rounding cases
        Arguments.of("usd", 1995L, 200L),
        Arguments.of("usd", 1994L, 199L));
  }

  @Test
  void convertBraintreeAmountToApiAmount() {
    assertThat(SubscriptionCurrencyUtil.convertBraintreeAmountToMinorUnits("USD", new BigDecimal("4.99")))
        .isEqualTo(499L);
    assertThat(SubscriptionCurrencyUtil.convertBraintreeAmountToMinorUnits("JPY", new BigDecimal("500")))
        .isEqualTo(500L);

    assertThatThrownBy(
        () -> SubscriptionCurrencyUtil.convertBraintreeAmountToMinorUnits("USD", new BigDecimal("4.995")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
