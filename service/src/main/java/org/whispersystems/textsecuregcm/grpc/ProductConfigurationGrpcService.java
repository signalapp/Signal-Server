package org.whispersystems.textsecuregcm.grpc;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.signal.chat.purchase.AmountList;
import org.signal.chat.purchase.BackupConfiguration;
import org.signal.chat.purchase.BackupLevelConfiguration;
import org.signal.chat.purchase.CurrencyConfiguration;
import org.signal.chat.purchase.GetConfigurationRequest;
import org.signal.chat.purchase.GetConfigurationResponse;
import org.signal.chat.purchase.LevelConfiguration;
import org.signal.chat.purchase.LoginConfiguration;
import org.signal.chat.purchase.SimpleProductConfigurationGrpc;
import org.whispersystems.textsecuregcm.configuration.LoginPurchaseConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptLevel;

public class ProductConfigurationGrpcService extends SimpleProductConfigurationGrpc.ProductConfigurationImplBase {
  private final GetConfigurationResponse configurationResponse;

  public ProductConfigurationGrpcService(
      final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      final LoginPurchaseConfiguration loginPurchaseConfiguration,
      List<CustomerAwareSubscriptionPaymentProcessor> paymentProcessors,
      final long backupMediaStorageAllowanceBytes) {
    this.configurationResponse = GetConfigurationResponse.newBuilder()
        .setBackup(buildBackupConfiguration(subscriptionConfiguration, backupMediaStorageAllowanceBytes))
        .setSepaMaximumEuros(oneTimeDonationConfiguration.sepaMaximumEuros().toString())
        .putAllCurrencies(buildCurrencyConfigurations(subscriptionConfiguration, oneTimeDonationConfiguration, paymentProcessors))
        .putAllBadgeLevels(buildLevelConfigurations(subscriptionConfiguration, oneTimeDonationConfiguration))
        .setLogin(buildLoginConfiguration(loginPurchaseConfiguration))
        .build();
  }

  @Override
  public GetConfigurationResponse getConfiguration(final GetConfigurationRequest request) {
    return this.configurationResponse;
  }

  private static Map<String, CurrencyConfiguration> buildCurrencyConfigurations(
      final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      List<CustomerAwareSubscriptionPaymentProcessor> paymentProcessors) {
    return SubscriptionsUtil
        .buildCurrencyConfiguration(paymentProcessors, oneTimeDonationConfiguration, subscriptionConfiguration)
        .entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> toProtoCurrencyConfiguration(e.getKey(), e.getValue())));
  }

  private static Map<Long, LevelConfiguration> buildLevelConfigurations(
      final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration) {
    final Map<Long, LevelConfiguration> donationLevels = new HashMap<>();
    subscriptionConfiguration.getDonationLevels().forEach((levelId, levelConfig) -> {
      donationLevels.put(levelId, LevelConfiguration.newBuilder().setBadgeId(levelConfig.badge()).build());
    });
    donationLevels.put(ReceiptLevel.ONE_TIME_DONATION.getValue(),
        LevelConfiguration.newBuilder()
            .setBadgeId(oneTimeDonationConfiguration.boost().badge())
            .setBadgeDurationSeconds(oneTimeDonationConfiguration.boost().expiration().toSeconds()).build());
    donationLevels.put(ReceiptLevel.ONE_TIME_GIFT_DONATION.getValue(),
        LevelConfiguration.newBuilder()
            .setBadgeId(oneTimeDonationConfiguration.gift().badge())
            .setBadgeDurationSeconds(oneTimeDonationConfiguration.gift().expiration().toSeconds()).build());
    return donationLevels;
  }

  private static BackupConfiguration buildBackupConfiguration(final SubscriptionConfiguration subscriptionConfiguration,
      final long backupMediaStorageAllowanceBytes) {
    final Map<Long, BackupLevelConfiguration> backupLevels =
        subscriptionConfiguration.getBackupLevels().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> BackupLevelConfiguration.newBuilder()
                .setStorageAllowanceBytes(backupMediaStorageAllowanceBytes)
                .setPlayProductId(e.getValue().playProductId())
                .setMediaTtlDays(e.getValue().mediaTtl().toDays())
                .build()));
    return BackupConfiguration.newBuilder()
        .putAllLevels(backupLevels)
        .setFreeTierMediaDays(subscriptionConfiguration.getbackupFreeTierMediaDuration().toDays())
        .build();
  }

  private static LoginConfiguration buildLoginConfiguration(
      final LoginPurchaseConfiguration loginPurchaseConfiguration) {
    return LoginConfiguration.newBuilder()
        .setLevel(ReceiptLevel.LOGIN.getValue())
        .setPlayProductId(loginPurchaseConfiguration.playProductId())
        .setPlayOptionId(loginPurchaseConfiguration.playOptionId())
        .setAppStoreProductId(loginPurchaseConfiguration.appStoreProductId())
        .build();
  }

  private static CurrencyConfiguration toProtoCurrencyConfiguration(
      final String currency,
      final org.whispersystems.textsecuregcm.subscriptions.CurrencyConfiguration config) {
    final CurrencyConfiguration.Builder builder = CurrencyConfiguration.newBuilder()
        .setMinimum(config.minimum().toString())
        .addAllSupportedPaymentMethods(config.supportedPaymentMethods().stream()
            .map(PaymentMethod::toProtoPaymentMethod)
            .toList());
    config.oneTime().forEach((levelId, amounts) ->
        builder.putOneTime(levelId, AmountList.newBuilder()
            .addAllAmounts(amounts.stream().map(BigDecimal::toString).toList())
            .build()));
    config.subscription()
        .forEach((levelId, amount) -> builder.putSubscription(levelId, amount.toString()));
    config.backupSubscription()
        .forEach((levelId, amount) -> builder.putBackupSubscription(levelId, amount.toString()));
    return builder.build();
  }
}
