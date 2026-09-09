package org.whispersystems.textsecuregcm.grpc;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.subscriptions.PaymentDetails;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptLevel;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil;

public class OneTimeDonationUtil {

  public static final String EURO_CURRENCY_CODE = "EUR";

  private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeDonationUtil.class);

  /// Thrown if a one time donation level cannot be parsed or if it is not found in configuration
  public static class InvalidLevelException extends Exception {

    public InvalidLevelException(final String message) {
      super(message);
    }
  }

  public record LocalizedPayPalDonationLineItem(Locale locale, String itemName){}
  public record DonationLevelDetails(ReceiptLevel level, Duration levelExpiration){}

  public sealed interface OneTimeDonationRequestValidationResult permits OneTimeDonationRequestValidationResult.Success,
      OneTimeDonationRequestValidationResult.UnsupportedCurrency,
      OneTimeDonationRequestValidationResult.UnsupportedLevel,
      OneTimeDonationRequestValidationResult.AmountBelowMinimum,
      OneTimeDonationRequestValidationResult.AmountAboveSepaLimit {

    record Success() implements OneTimeDonationRequestValidationResult {}

    record UnsupportedCurrency() implements OneTimeDonationRequestValidationResult {}

    record UnsupportedLevel() implements OneTimeDonationRequestValidationResult {}

    record AmountBelowMinimum(BigDecimal minimum) implements OneTimeDonationRequestValidationResult {}

    record AmountAboveSepaLimit(BigDecimal maximum) implements OneTimeDonationRequestValidationResult {}

  }

  public static OneTimeDonationRequestValidationResult validateOneTimeDonationRequest(
      final String currency,
      final long amount,
      final long level,
      final PaymentMethod paymentMethod,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final CustomerAwareSubscriptionPaymentProcessor manager
  ) {

    if (ReceiptLevel.lookupLevel(level)
        .map(rl -> rl != ReceiptLevel.ONE_TIME_DONATION && rl != ReceiptLevel.ONE_TIME_GIFT_DONATION)
        .orElse(true)) {
      return new OneTimeDonationRequestValidationResult.UnsupportedLevel();
    }

    if (!manager.getSupportedCurrenciesForPaymentMethod(paymentMethod)
        .contains(currency.toLowerCase(Locale.ROOT))) {
      return new OneTimeDonationRequestValidationResult.UnsupportedCurrency();
    }

    final BigDecimal minCurrencyAmount =
        oneTimeDonationConfiguration.currencies().get(currency.toLowerCase(Locale.ROOT)).minimum();
    if (SubscriptionCurrencyUtil.convertPrimaryToMinorUnits(currency, minCurrencyAmount) > amount) {
      return new OneTimeDonationRequestValidationResult.AmountBelowMinimum(minCurrencyAmount);
    }

    if (paymentMethod == PaymentMethod.SEPA_DEBIT) {
      final BigDecimal sepaMaximumEuros = oneTimeDonationConfiguration.sepaMaximumEuros();
      if (amount > SubscriptionCurrencyUtil.convertPrimaryToMinorUnits(EURO_CURRENCY_CODE, sepaMaximumEuros)) {
        return new OneTimeDonationRequestValidationResult.AmountAboveSepaLimit(sepaMaximumEuros);
      }
    }
    return new OneTimeDonationRequestValidationResult.Success();
  }

  public static LocalizedPayPalDonationLineItem localizePayPalDonationLineItem(
      final PayPalDonationsTranslator payPalDonationsTranslator, final List<Locale> acceptableLocales) {
    // These two localizations are a best-effort, and it's possible that the first `locale` and the localized line
    // item name will not match. We could try to align with the locales PayPal documents <https://developer.paypal.com/reference/locale-codes/#supported-locale-codes>
    // but that's a moving target, and we can hopefully have one of them be better for the user by selecting
    // independently.
    final Locale locale = SubscriptionsUtil.getPayPalLocale(acceptableLocales);
    final String localizedLineItemName = payPalDonationsTranslator.translate(acceptableLocales,
        org.whispersystems.textsecuregcm.subscriptions.PayPalDonationsTranslator.ONE_TIME_DONATION_LINE_ITEM_KEY);
    return new LocalizedPayPalDonationLineItem(locale, localizedLineItemName);
  }

  public static DonationLevelDetails getLevelDetails(final PaymentDetails paymentDetails,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration)
      throws InvalidLevelException {

    final Duration levelExpiration = switch (paymentDetails.level()) {
      case ONE_TIME_DONATION -> oneTimeDonationConfiguration.boost().expiration();
      case ONE_TIME_GIFT_DONATION -> oneTimeDonationConfiguration.gift().expiration();
      default -> {
        LOGGER.error("level ({}) returned from payment intent that is unknown to the server", paymentDetails.level());
        throw new InvalidLevelException("unrecognized level");
      }
    };
    return new DonationLevelDetails(paymentDetails.level(), levelExpiration);
  }

}
