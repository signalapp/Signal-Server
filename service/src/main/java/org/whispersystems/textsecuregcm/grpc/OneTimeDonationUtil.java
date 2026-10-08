package org.whispersystems.textsecuregcm.grpc;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.purchases.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.purchases.PayPalDonationsTranslator;
import org.whispersystems.textsecuregcm.purchases.PaymentDetails;
import org.whispersystems.textsecuregcm.purchases.PaymentMethod;
import org.whispersystems.textsecuregcm.purchases.ReceiptLevel;
import org.whispersystems.textsecuregcm.purchases.SubscriptionCurrencyUtil;

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

  /// Thrown when a one-time donation request that failed validation
  public static class ValidationException extends Exception {
    /// A human-readable description of the failure, suitable for logging
    private final String description;
    /// The fields of the REST error response body
    private final Map<String, String> errorBody;

    public ValidationException(final String description, final Map<String, String> errorBody) {
      this.description = description;
      this.errorBody = errorBody;
    }

    public String getDescription() {
      return description;
    }

    public Map<String, String> getErrorBody() {
      return errorBody;
    }
  }

  private static boolean isOneTimeDonationLevel(final long level) {
    return ReceiptLevel.lookupLevel(level)
        .map(rl -> rl == ReceiptLevel.ONE_TIME_DONATION || rl == ReceiptLevel.ONE_TIME_GIFT_DONATION)
        .orElse(false);
  }

  /// Validates a request is for a valid one-time donation level and is at least the configured minimum for the currency
  /// and payment method.
  ///
  /// @return the validation error, or empty if the request is valid
  public static void validateOneTimeDonationRequest(
      final String currency,
      final long amount,
      final long level,
      final PaymentMethod paymentMethod,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final CustomerAwareSubscriptionPaymentProcessor manager
  ) throws ValidationException {

    if (!isOneTimeDonationLevel(level)) {
      throw new ValidationException("Not a one-time donation level", Map.of("error", "invalid_level"));
    }

    if (!manager.getSupportedCurrenciesForPaymentMethod(paymentMethod)
        .contains(currency.toLowerCase(Locale.ROOT))) {
      throw new ValidationException(
          "Currency '" + currency + "' is not supported for payment method " + paymentMethod,
          Map.of("error", "unsupported_currency"));
    }

    final BigDecimal minCurrencyAmount =
        oneTimeDonationConfiguration.currencies().get(currency.toLowerCase(Locale.ROOT)).minimum();
    if (SubscriptionCurrencyUtil.convertPrimaryToMinorUnits(currency, minCurrencyAmount) > amount) {
      throw new ValidationException(
          "Amount is below the minimum for currency '" + currency + "'",
          Map.of("error", "amount_below_currency_minimum", "minimum", minCurrencyAmount.toString()));
    }

    if (paymentMethod == PaymentMethod.SEPA_DEBIT) {
      final BigDecimal sepaMaximumEuros = oneTimeDonationConfiguration.sepaMaximumEuros();
      if (amount > SubscriptionCurrencyUtil.convertPrimaryToMinorUnits(EURO_CURRENCY_CODE, sepaMaximumEuros)) {
        throw new ValidationException("Amount is above the SEPA Direct Debit maximum",
            Map.of("error", "amount_above_sepa_limit", "maximum", sepaMaximumEuros.toString()));
      }
    }
  }

  public static LocalizedPayPalDonationLineItem localizePayPalDonationLineItem(
      final PayPalDonationsTranslator payPalDonationsTranslator, final List<Locale> acceptableLocales) {
    // These two localizations are a best-effort, and it's possible that the first `locale` and the localized line
    // item name will not match. We could try to align with the locales PayPal documents <https://developer.paypal.com/reference/locale-codes/#supported-locale-codes>
    // but that's a moving target, and we can hopefully have one of them be better for the user by selecting
    // independently.
    final Locale locale = SubscriptionsUtil.getPayPalLocale(acceptableLocales);
    final String localizedLineItemName = payPalDonationsTranslator.translate(acceptableLocales,
        org.whispersystems.textsecuregcm.purchases.PayPalDonationsTranslator.ONE_TIME_DONATION_LINE_ITEM_KEY);
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
