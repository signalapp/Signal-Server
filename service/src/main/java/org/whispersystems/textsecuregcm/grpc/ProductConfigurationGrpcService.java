package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.signal.chat.purchase.AmountList;
import org.signal.chat.purchase.BackupConfiguration;
import org.signal.chat.purchase.BackupLevelConfiguration;
import org.signal.chat.purchase.Configuration;
import org.signal.chat.purchase.CurrencyConfiguration;
import org.signal.chat.purchase.GetConfigurationRequest;
import org.signal.chat.purchase.GetConfigurationResponse;
import org.signal.chat.purchase.LevelConfiguration;
import org.signal.chat.purchase.SimpleProductConfigurationGrpc;
import org.signal.chat.purchase.TaggedConfiguration;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.entities.PurchasableBadge;
import org.whispersystems.textsecuregcm.subscriptions.CustomerAwareSubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;

public class ProductConfigurationGrpcService extends SimpleProductConfigurationGrpc.ProductConfigurationImplBase {

  private final SubscriptionConfiguration subscriptionConfiguration;
  private final OneTimeDonationConfiguration oneTimeDonationConfiguration;
  private final BadgeTranslator badgeTranslator;

  // These portions of the configuration are identical on every request
  private final Map<String, CurrencyConfiguration> currencyConfigurations;
  private final BackupConfiguration backupConfiguration;

  // The configuration varies based on the provided Accept-Language header. Here we cache the eTag for the resolved
  // language, so if the caller provides a matching etag we don't have to build the full configuration match.
  private final ConcurrentHashMap<Locale, ByteString> localeToEtag = new ConcurrentHashMap<>();


  public ProductConfigurationGrpcService(
      final SubscriptionConfiguration subscriptionConfiguration,
      final OneTimeDonationConfiguration oneTimeDonationConfiguration,
      List<CustomerAwareSubscriptionPaymentProcessor> paymentProcessors,
      final BadgeTranslator badgeTranslator,
      final long backupMediaStorageAllowanceBytes) {
    this.subscriptionConfiguration = subscriptionConfiguration;
    this.oneTimeDonationConfiguration = oneTimeDonationConfiguration;
    this.badgeTranslator = badgeTranslator;
    this.backupConfiguration = buildBackupConfiguration(subscriptionConfiguration, backupMediaStorageAllowanceBytes);
    this.currencyConfigurations =
        buildCurrencyConfigurations(subscriptionConfiguration, oneTimeDonationConfiguration, paymentProcessors);
  }

  @Override
  public GetConfigurationResponse getConfiguration(final GetConfigurationRequest request) {
    final Locale locale = badgeTranslator.resolveLocale(RequestAttributesUtil.getAvailableAcceptedLocales());
    final ByteString etag = localeToEtag.get(locale);
    if (etag != null && etag.equals(request.getEtag())) {
      return GetConfigurationResponse.newBuilder().setEtagMatched(true).build();
    }

    final Map<Long, LevelConfiguration> levelConfigurations = SubscriptionsUtil
        .buildDonationLevelsConfiguration(subscriptionConfiguration, oneTimeDonationConfiguration, badgeTranslator,
            RequestAttributesUtil.getAvailableAcceptedLocales())
        .entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> toProtoLevelConfiguration(entry.getValue())));

    final Configuration configuration = Configuration.newBuilder()
        .putAllCurrencies(currencyConfigurations)
        .putAllBadgeLevels(levelConfigurations)
        .setBackup(backupConfiguration)
        .setSepaMaximumEuros(oneTimeDonationConfiguration.sepaMaximumEuros().toString()).build();
    final TaggedConfiguration taggedConfiguration = calculateEtag(configuration);

    // This could race and multiple threads could decide to build-and-cache. That's fine, they should all calculate
    // the same etag.
    localeToEtag.put(locale, taggedConfiguration.getEtag());
    return GetConfigurationResponse.newBuilder().setTaggedConfiguration(taggedConfiguration).build();
  }

  private static TaggedConfiguration calculateEtag(final Configuration configuration) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(configuration.getSerializedSize());
    final CodedOutputStream cos = CodedOutputStream.newInstance(baos);
    cos.useDeterministicSerialization();
    try {
      configuration.writeTo(cos);
      cos.flush();
      final byte[] configurationBytes = baos.toByteArray();
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(configurationBytes);
      return TaggedConfiguration.newBuilder()
          .setConfiguration(configuration)
          .setEtag(ByteString.copyFrom(hash))
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
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

  private static LevelConfiguration toProtoLevelConfiguration(
      final org.whispersystems.textsecuregcm.subscriptions.LevelConfiguration levelConfiguration) {
    final LevelConfiguration.Builder builder = LevelConfiguration.newBuilder();
    if (levelConfiguration.badge() instanceof final PurchasableBadge purchasableBadge) {
      builder.setBadgeDurationSeconds(purchasableBadge.getDuration().toSeconds());
    }
    return builder
        .setBadge(BadgeGrpcHelper.toGrpcBadge(levelConfiguration.badge()))
        .build();
  }
}
