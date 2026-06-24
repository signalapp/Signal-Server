package org.whispersystems.textsecuregcm.grpc;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationCurrencyConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionLevelConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.subscriptions.CurrencyConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.LevelConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class SubscriptionsUtil {

  private static final String DONATION_PERMIT_PRESENT_COUNTER_NAME = MetricsUtil.name(SubscriptionsUtil.class,
      "donationPermitPresent");

  private static final String DONATION_PERMIT_SPEND_COUNTER_NAME = MetricsUtil.name(SubscriptionsUtil.class,
      "donationPermitSpend");

  @Nullable
  public static ClientPlatform getClientPlatform(@Nullable final String userAgentString) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgentString).platform();
    } catch (final UnrecognizedUserAgentException e) {
      return null;
    }
  }

  public static boolean subscriptionsAreSameType(final SubscriptionConfiguration subscriptionConfiguration,  final long level1, final long level2) {
    return subscriptionConfiguration.getSubscriptionLevel(level1).type()
        == subscriptionConfiguration.getSubscriptionLevel(level2).type();
  }

  public static Instant receiptExpirationWithGracePeriod(
      final SubscriptionConfiguration subscriptionConfiguration,
      final CustomerAwareSubscriptionPaymentProcessor.ReceiptItem receiptItem) {
    return switch (subscriptionConfiguration.getSubscriptionLevel(receiptItem.level()).type()) {
      case DONATION -> receiptItem.paymentTime().receiptExpiration(
          subscriptionConfiguration.getBadgeExpiration(),
          subscriptionConfiguration.getBadgeGracePeriod());
      case BACKUP -> receiptItem.paymentTime().receiptExpiration(
          subscriptionConfiguration.getBackupExpiration(),
          subscriptionConfiguration.getBackupGracePeriod());
    };
  }

  public static Map<String, CurrencyConfiguration> buildCurrencyConfiguration(final List<CustomerAwareSubscriptionPaymentProcessor> subscriptionPaymentProcessors,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final SubscriptionConfiguration subscriptionConfiguration) {
    return oneTimeDonationConfiguration.currencies()
        .entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, currencyAndConfig -> {
          final String currency = currencyAndConfig.getKey();
          final OneTimeDonationCurrencyConfiguration currencyConfig = currencyAndConfig.getValue();

          final Map<Long, List<BigDecimal>> oneTimeLevelsToSuggestedAmounts = Map.of(
              oneTimeDonationConfiguration.boost().level(), currencyConfig.boosts(),
              oneTimeDonationConfiguration.gift().level(), List.of(currencyConfig.gift())
          );

          final Function<Map<Long, ? extends SubscriptionLevelConfiguration>, Map<Long, BigDecimal>> extractSubscriptionAmounts = levels ->
              levels.entrySet().stream()
                  .filter(levelIdAndConfig -> levelIdAndConfig.getValue().prices().containsKey(currency))
                  .collect(Collectors.toMap(
                      Map.Entry::getKey,
                      levelIdAndConfig -> levelIdAndConfig.getValue().prices().get(currency).amount()));

          final List<PaymentMethod> supportedPaymentMethods = Arrays.stream(PaymentMethod.values())
              .filter(paymentMethod -> subscriptionPaymentProcessors.stream()
                  .anyMatch(manager -> manager.supportsPaymentMethod(paymentMethod)
                      && manager.getSupportedCurrenciesForPaymentMethod(paymentMethod).contains(currency)))
              .collect(Collectors.toList());

          if (supportedPaymentMethods.isEmpty()) {
            throw new RuntimeException("Configuration has currency with no processor support: " + currency);
          }

          return new CurrencyConfiguration(
              currencyConfig.minimum(),
              oneTimeLevelsToSuggestedAmounts,
              extractSubscriptionAmounts.apply(subscriptionConfiguration.getDonationLevels()),
              extractSubscriptionAmounts.apply(subscriptionConfiguration.getBackupLevels()),
              supportedPaymentMethods);
        }));
  }

  public static Map<Long, LevelConfiguration> buildDonationLevelsConfiguration(
      final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final BadgeTranslator badgeTranslator,
      final List<Locale> acceptableLanguages) {
    final Map<Long, LevelConfiguration> donationLevels = new HashMap<>();

    subscriptionConfiguration.getDonationLevels().forEach((levelId, levelConfig) -> {
      final LevelConfiguration levelConfiguration = new LevelConfiguration(
          badgeTranslator.translate(acceptableLanguages, levelConfig.badge()));
      donationLevels.put(levelId, levelConfiguration);
    });

    final Badge boostBadge = badgeTranslator.translate(acceptableLanguages,
        oneTimeDonationConfiguration.boost().badge());
    donationLevels.put(oneTimeDonationConfiguration.boost().level(),
        new LevelConfiguration(
            // NB: the one-time badges are PurchasableBadge, which has a `duration` field
            new PurchasableBadge(
                boostBadge,
                oneTimeDonationConfiguration.boost().expiration())));

    final Badge giftBadge = badgeTranslator.translate(acceptableLanguages, oneTimeDonationConfiguration.gift().badge());
    donationLevels.put(oneTimeDonationConfiguration.gift().level(),
        new LevelConfiguration(
            new PurchasableBadge(
                giftBadge,
                oneTimeDonationConfiguration.gift().expiration())));
    return donationLevels;
  }

  /// Verifies the permit and spends it
  ///
  /// @return `true` if the spend was accepted, otherwise `false`
  /// @throws VerificationFailedException if the permit is invalid
  public static boolean verifyAndSpendDonationPermit(final DonationPermit permit, final DonationPermitsManager donationPermitsManager, final Clock clock) throws VerificationFailedException {

    final boolean spent = donationPermitsManager.spend(permit);

    Metrics.counter(DONATION_PERMIT_SPEND_COUNTER_NAME, "outcome", spent ? "success" : "failure").increment();

    return spent;
  }

  public static void recordDonationPermitPresent(final boolean present, final String context, final String userAgent) {
    Metrics.counter(DONATION_PERMIT_PRESENT_COUNTER_NAME,
        Tags.of("present", String.valueOf(present),
                "context", context)
            .and(UserAgentTagUtil.getPlatformTag(userAgent)))
        .increment();
  }

}
