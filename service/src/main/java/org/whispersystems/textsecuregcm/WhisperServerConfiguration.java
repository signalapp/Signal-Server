/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.AbusiveMessageFilterConfiguration;
import org.whispersystems.textsecuregcm.configuration.AccountDatabaseCrawlerConfiguration;
import org.whispersystems.textsecuregcm.configuration.AdminEventLoggingConfiguration;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.configuration.AppConfigConfiguration;
import org.whispersystems.textsecuregcm.configuration.AwsAttachmentsConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.BraintreeConfiguration;
import org.whispersystems.textsecuregcm.configuration.CdnConfiguration;
import org.whispersystems.textsecuregcm.configuration.DatadogConfiguration;
import org.whispersystems.textsecuregcm.configuration.DirectoryConfiguration;
import org.whispersystems.textsecuregcm.configuration.DirectoryV2Configuration;
import org.whispersystems.textsecuregcm.configuration.DynamoDbClientConfiguration;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables;
import org.whispersystems.textsecuregcm.configuration.FcmConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcpAttachmentsConfiguration;
import org.whispersystems.textsecuregcm.configuration.HCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.MaxDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.MessageCacheConfiguration;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.ArtServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RecaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.configuration.RegistrationServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.RemoteConfigConfiguration;
import org.whispersystems.textsecuregcm.configuration.ReportMessageConfiguration;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.StripeConfiguration;
import org.whispersystems.textsecuregcm.configuration.SubscriptionConfiguration;
import org.whispersystems.textsecuregcm.configuration.TestDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.UnidentifiedDeliveryConfiguration;
import org.whispersystems.textsecuregcm.configuration.UsernameConfiguration;
import org.whispersystems.textsecuregcm.configuration.VoiceVerificationConfiguration;
import org.whispersystems.textsecuregcm.configuration.ZkConfig;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;

/** @noinspection MismatchedQueryAndUpdateOfCollection, WeakerAccess */
public class WhisperServerConfiguration extends Configuration {

  @NotNull
  @Valid
  @JsonProperty
  private AdminEventLoggingConfiguration adminEventLoggingConfiguration;

  @NotNull
  @Valid
  @JsonProperty
  private StripeConfiguration stripe;

  @NotNull
  @Valid
  @JsonProperty
  private BraintreeConfiguration braintree;

  @NotNull
  @Valid
  @JsonProperty
  private DynamoDbClientConfiguration dynamoDbClientConfiguration;

  @NotNull
  @Valid
  @JsonProperty
  private DynamoDbTables dynamoDbTables;

  @NotNull
  @Valid
  @JsonProperty
  private AwsAttachmentsConfiguration awsAttachments;

  @NotNull
  @Valid
  @JsonProperty
  private GcpAttachmentsConfiguration gcpAttachments;

  @NotNull
  @Valid
  @JsonProperty
  private CdnConfiguration cdn;

  @NotNull
  @Valid
  @JsonProperty
  private DatadogConfiguration datadog;

  @NotNull
  @Valid
  @JsonProperty
  private RedisClusterConfiguration cacheCluster;

  @NotNull
  @Valid
  @JsonProperty
  private RedisConfiguration pubsub;

  @NotNull
  @Valid
  @JsonProperty
  private RedisClusterConfiguration metricsCluster;

  @NotNull
  @Valid
  @JsonProperty
  private DirectoryConfiguration directory;

  @NotNull
  @Valid
  @JsonProperty
  private DirectoryV2Configuration directoryV2;

  @NotNull
  @Valid
  @JsonProperty
  private AccountDatabaseCrawlerConfiguration accountDatabaseCrawler;

  @NotNull
  @Valid
  @JsonProperty
  private RedisClusterConfiguration pushSchedulerCluster;

  @NotNull
  @Valid
  @JsonProperty
  private RedisClusterConfiguration rateLimitersCluster;

  @NotNull
  @Valid
  @JsonProperty
  private MessageCacheConfiguration messageCache;

  @NotNull
  @Valid
  @JsonProperty
  private RedisClusterConfiguration clientPresenceCluster;

  @Valid
  @NotNull
  @JsonProperty
  private List<TestDeviceConfiguration> testDevices = new LinkedList<>();

  @Valid
  @NotNull
  @JsonProperty
  private List<MaxDeviceConfiguration> maxDevices = new LinkedList<>();

  @Valid
  @NotNull
  @JsonProperty
  private RateLimitsConfiguration limits = new RateLimitsConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private WebSocketConfiguration webSocket = new WebSocketConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private FcmConfiguration fcm;

  @Valid
  @NotNull
  @JsonProperty
  private ApnConfiguration apn;

  @Valid
  @NotNull
  @JsonProperty
  private UnidentifiedDeliveryConfiguration unidentifiedDelivery;

  @Valid
  @NotNull
  @JsonProperty
  private VoiceVerificationConfiguration voiceVerification;

  @Valid
  @NotNull
  @JsonProperty
  private RecaptchaConfiguration recaptcha;

  @Valid
  @NotNull
  @JsonProperty
  private HCaptchaConfiguration hCaptcha;

  @Valid
  @NotNull
  @JsonProperty
  private SecureStorageServiceConfiguration storageService;

  @Valid
  @NotNull
  @JsonProperty
  private SecureBackupServiceConfiguration backupService;

  @Valid
  @NotNull
  @JsonProperty
  private PaymentsServiceConfiguration paymentsService;

  @Valid
  @NotNull
  @JsonProperty
  private ArtServiceConfiguration artService;

  @Valid
  @NotNull
  @JsonProperty
  private ZkConfig zkConfig;

  @Valid
  @NotNull
  @JsonProperty
  private RemoteConfigConfiguration remoteConfig;

  @Valid
  @NotNull
  @JsonProperty
  private AppConfigConfiguration appConfig;

  @Valid
  @NotNull
  @JsonProperty
  private BadgesConfiguration badges;

  @Valid
  @JsonProperty
  @NotNull
  private SubscriptionConfiguration subscription;

  @Valid
  @JsonProperty
  @NotNull
  private OneTimeDonationConfiguration oneTimeDonations;

  @Valid
  @NotNull
  @JsonProperty
  private ReportMessageConfiguration reportMessage = new ReportMessageConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private UsernameConfiguration username = new UsernameConfiguration();

  @Valid
  @JsonProperty
  private AbusiveMessageFilterConfiguration abusiveMessageFilter;

  @Valid
  @NotNull
  @JsonProperty
  private RegistrationServiceConfiguration registrationService;

  public AdminEventLoggingConfiguration getAdminEventLoggingConfiguration() {
    return adminEventLoggingConfiguration;
  }

  public StripeConfiguration getStripe() {
    return stripe;
  }

  public BraintreeConfiguration getBraintree() {
    return braintree;
  }

  public DynamoDbClientConfiguration getDynamoDbClientConfiguration() {
    return dynamoDbClientConfiguration;
  }

  public DynamoDbTables getDynamoDbTables() {
    return dynamoDbTables;
  }

  public RecaptchaConfiguration getRecaptchaConfiguration() {
    return recaptcha;
  }

  public HCaptchaConfiguration getHCaptchaConfiguration() {
    return hCaptcha;
  }

  public VoiceVerificationConfiguration getVoiceVerificationConfiguration() {
    return voiceVerification;
  }

  public WebSocketConfiguration getWebSocketConfiguration() {
    return webSocket;
  }

  public AwsAttachmentsConfiguration getAwsAttachmentsConfiguration() {
    return awsAttachments;
  }

  public GcpAttachmentsConfiguration getGcpAttachmentsConfiguration() {
    return gcpAttachments;
  }

  public RedisClusterConfiguration getCacheClusterConfiguration() {
    return cacheCluster;
  }

  public RedisConfiguration getPubsubCacheConfiguration() {
    return pubsub;
  }

  public RedisClusterConfiguration getMetricsClusterConfiguration() {
    return metricsCluster;
  }

  public DirectoryConfiguration getDirectoryConfiguration() {
    return directory;
  }

  public DirectoryV2Configuration getDirectoryV2Configuration() {
    return directoryV2;
  }

  public SecureStorageServiceConfiguration getSecureStorageServiceConfiguration() {
    return storageService;
  }

  public AccountDatabaseCrawlerConfiguration getAccountDatabaseCrawlerConfiguration() {
    return accountDatabaseCrawler;
  }

  public MessageCacheConfiguration getMessageCacheConfiguration() {
    return messageCache;
  }

  public RedisClusterConfiguration getClientPresenceClusterConfiguration() {
    return clientPresenceCluster;
  }

  public RedisClusterConfiguration getPushSchedulerCluster() {
    return pushSchedulerCluster;
  }

  public RedisClusterConfiguration getRateLimitersCluster() {
    return rateLimitersCluster;
  }

  public RateLimitsConfiguration getLimitsConfiguration() {
    return limits;
  }

  public FcmConfiguration getFcmConfiguration() {
    return fcm;
  }

  public ApnConfiguration getApnConfiguration() {
    return apn;
  }

  public CdnConfiguration getCdnConfiguration() {
    return cdn;
  }

  public DatadogConfiguration getDatadogConfiguration() {
    return datadog;
  }

  public UnidentifiedDeliveryConfiguration getDeliveryCertificate() {
    return unidentifiedDelivery;
  }

  public Map<String, Integer> getTestDevices() {
    Map<String, Integer> results = new HashMap<>();

    for (TestDeviceConfiguration testDeviceConfiguration : testDevices) {
      results.put(testDeviceConfiguration.getNumber(),
                  testDeviceConfiguration.getCode());
    }

    return results;
  }

  public Map<String, Integer> getMaxDevices() {
    Map<String, Integer> results = new HashMap<>();

    for (MaxDeviceConfiguration maxDeviceConfiguration : maxDevices) {
      results.put(maxDeviceConfiguration.getNumber(),
                  maxDeviceConfiguration.getCount());
    }

    return results;
  }

  public SecureBackupServiceConfiguration getSecureBackupServiceConfiguration() {
    return backupService;
  }

  public PaymentsServiceConfiguration getPaymentsServiceConfiguration() {
    return paymentsService;
  }

  public ArtServiceConfiguration getArtServiceConfiguration() {
    return artService;
  }

  public ZkConfig getZkConfig() {
    return zkConfig;
  }

  public RemoteConfigConfiguration getRemoteConfigConfiguration() {
    return remoteConfig;
  }

  public AppConfigConfiguration getAppConfig() {
    return appConfig;
  }

  public BadgesConfiguration getBadges() {
    return badges;
  }

  public SubscriptionConfiguration getSubscription() {
    return subscription;
  }

  public OneTimeDonationConfiguration getOneTimeDonations() {
    return oneTimeDonations;
  }

  public ReportMessageConfiguration getReportMessageConfiguration() {
    return reportMessage;
  }

  public AbusiveMessageFilterConfiguration getAbusiveMessageFilterConfiguration() {
    return abusiveMessageFilter;
  }

  public UsernameConfiguration getUsername() {
    return username;
  }

  public RegistrationServiceConfiguration getRegistrationServiceConfiguration() {
    return registrationService;
  }
}
