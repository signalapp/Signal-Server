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
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionCurrencyUtil;

public class OneTimeDonationUtil {

  private static final String EURO_CURRENCY_CODE = "EUR";

  private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeDonationUtil.class);

  /// Thrown if a one time donation level cannot be parsed or if it is not found in configuration
  public static class InvalidLevelException extends Exception {

    public InvalidLevelException(final String message) {
      super(message);
    }
  }

  public record LocalizedPayPalDonationLineItem(Locale locale, String itemName){}
  public record DonationLevelDetails(long level, Duration levelExpiration){}

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
      final BigDecimal amount,
      final long level,
      final PaymentMethod paymentMethod,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final CustomerAwareSubscriptionPaymentProcessor manager
  ) {

    if (!(level == oneTimeDonationConfiguration.gift().level()
        || level == oneTimeDonationConfiguration.boost().level())) {
      return new OneTimeDonationRequestValidationResult.UnsupportedLevel();
    }

    if (!manager.getSupportedCurrenciesForPaymentMethod(paymentMethod)
        .contains(currency.toLowerCase(Locale.ROOT))) {
      return new OneTimeDonationRequestValidationResult.UnsupportedCurrency();
    }

    final BigDecimal minCurrencyAmountMajorUnits = oneTimeDonationConfiguration.currencies()
        .get(currency.toLowerCase(Locale.ROOT)).minimum();
    final BigDecimal minCurrencyAmountMinorUnits = SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(
        currency,
        minCurrencyAmountMajorUnits);
    if (minCurrencyAmountMinorUnits.compareTo(amount) > 0) {
      return new OneTimeDonationRequestValidationResult.AmountBelowMinimum(minCurrencyAmountMajorUnits);
    }

    if (paymentMethod == PaymentMethod.SEPA_DEBIT &&
        amount.compareTo(SubscriptionCurrencyUtil.convertConfiguredAmountToApiAmount(
            EURO_CURRENCY_CODE,
            oneTimeDonationConfiguration.sepaMaximumEuros())) > 0) {
      return new OneTimeDonationRequestValidationResult.AmountAboveSepaLimit(
          oneTimeDonationConfiguration.sepaMaximumEuros());
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

    long level = oneTimeDonationConfiguration.boost().level();
    if (paymentDetails.customMetadata() != null) {
      final String levelMetadata = paymentDetails.customMetadata()
          .getOrDefault("level", Long.toString(oneTimeDonationConfiguration.boost().level()));
      try {
        level = Long.parseLong(levelMetadata);
      } catch (final NumberFormatException e) {
        LOGGER.error("failed to parse level metadata ({}) on payment intent {}", levelMetadata,
            paymentDetails.id(), e);
        throw new InvalidLevelException("failed to parse level metadata");
      }
    }

    final Duration levelExpiration;
    if (level == oneTimeDonationConfiguration.boost().level()) {
      levelExpiration = oneTimeDonationConfiguration.boost().expiration();
    } else if (level == oneTimeDonationConfiguration.gift().level()) {
      levelExpiration = oneTimeDonationConfiguration.gift().expiration();
    } else {
      LOGGER.error("level ({}) returned from payment intent that is unknown to the server", level);
      throw new InvalidLevelException("unrecognized level");
    }
    return new DonationLevelDetails(level, levelExpiration);
  }

}
