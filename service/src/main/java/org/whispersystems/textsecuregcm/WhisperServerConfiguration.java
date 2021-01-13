/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;
import org.whispersystems.textsecuregcm.configuration.AccountDatabaseCrawlerConfiguration;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.configuration.AwsAttachmentsConfiguration;
import org.whispersystems.textsecuregcm.configuration.CdnConfiguration;
import org.whispersystems.textsecuregcm.configuration.DatabaseConfiguration;
import org.whispersystems.textsecuregcm.configuration.DirectoryConfiguration;
import org.whispersystems.textsecuregcm.configuration.FeatureFlagConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcmConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcpAttachmentsConfiguration;
import org.whispersystems.textsecuregcm.configuration.AccountsDatabaseConfiguration;
import org.whispersystems.textsecuregcm.configuration.MaxDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.MessageCacheConfiguration;
import org.whispersystems.textsecuregcm.configuration.MicrometerConfiguration;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.PushConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RecaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisClusterConfiguration;
import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.configuration.RemoteConfigConfiguration;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.TestDeviceConfiguration;
import org.whispersystems.textsecuregcm.configuration.TurnConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.configuration.UnidentifiedDeliveryConfiguration;
import org.whispersystems.textsecuregcm.configuration.VoiceVerificationConfiguration;
import org.whispersystems.textsecuregcm.configuration.ZkConfig;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** @noinspection MismatchedQueryAndUpdateOfCollection, WeakerAccess */
public class WhisperServerConfiguration extends Configuration {

  @NotNull
  @Valid
  @JsonProperty
  private TwilioConfiguration twilio;

  @NotNull
  @Valid
  @JsonProperty
  private PushConfiguration push;

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
  private MicrometerConfiguration micrometer;

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
  private AccountDatabaseCrawlerConfiguration accountDatabaseCrawler;

  @NotNull
  @Valid
  @JsonProperty
  private RedisConfiguration pushScheduler;

  @NotNull
  @Valid
  @JsonProperty
  private MessageCacheConfiguration messageCache;

  @Valid
  @NotNull
  @JsonProperty
  private DatabaseConfiguration messageStore;

  @Valid
  @NotNull
  @JsonProperty
  private DatabaseConfiguration abuseDatabase;

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
  private AccountsDatabaseConfiguration accountsDatabase;

  @Valid
  @NotNull
  @JsonProperty
  private RateLimitsConfiguration limits = new RateLimitsConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private WebSocketConfiguration webSocket = new WebSocketConfiguration();

  @Valid
  @NotNull
  @JsonProperty
  private TurnConfiguration turn;

  @Valid
  @NotNull
  @JsonProperty
  private GcmConfiguration gcm;

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
  private ZkConfig zkConfig;

  @Valid
  @NotNull
  @JsonProperty
  private RemoteConfigConfiguration remoteConfig;

  @Valid
  @NotNull
  @JsonProperty
  private FeatureFlagConfiguration featureFlag;

  private Map<String, String> transparentDataIndex = new HashMap<>();

  public RecaptchaConfiguration getRecaptchaConfiguration() {
    return recaptcha;
  }

  public VoiceVerificationConfiguration getVoiceVerificationConfiguration() {
    return voiceVerification;
  }

  public WebSocketConfiguration getWebSocketConfiguration() {
    return webSocket;
  }

  public TwilioConfiguration getTwilioConfiguration() {
    return twilio;
  }

  public PushConfiguration getPushConfiguration() {
    return push;
  }

  public JerseyClientConfiguration getJerseyClientConfiguration() {
    return httpClient;
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

  public SecureStorageServiceConfiguration getSecureStorageServiceConfiguration() {
    return storageService;
  }

  public AccountDatabaseCrawlerConfiguration getAccountDatabaseCrawlerConfiguration() {
    return accountDatabaseCrawler;
  }

  public MessageCacheConfiguration getMessageCacheConfiguration() {
    return messageCache;
  }

  public RedisConfiguration getPushScheduler() {
    return pushScheduler;
  }

  public DatabaseConfiguration getMessageStoreConfiguration() {
    return messageStore;
  }

  public DatabaseConfiguration getAbuseDatabaseConfiguration() {
    return abuseDatabase;
  }

  public AccountsDatabaseConfiguration getAccountsDatabaseConfiguration() {
    return accountsDatabase;
  }

  public RateLimitsConfiguration getLimitsConfiguration() {
    return limits;
  }

  public TurnConfiguration getTurnConfiguration() {
    return turn;
  }

  public GcmConfiguration getGcmConfiguration() {
    return gcm;
  }

  public ApnConfiguration getApnConfiguration() {
    return apn;
  }

  public CdnConfiguration getCdnConfiguration() {
    return cdn;
  }

  public MicrometerConfiguration getMicrometerConfiguration() {
    return micrometer;
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

  public Map<String, String> getTransparentDataIndex() {
    return transparentDataIndex;
  }

  public SecureBackupServiceConfiguration getSecureBackupServiceConfiguration() {
    return backupService;
  }

  public PaymentsServiceConfiguration getPaymentsServiceConfiguration() {
    return paymentsService;
  }

  public ZkConfig getZkConfig() {
    return zkConfig;
  }

  public RemoteConfigConfiguration getRemoteConfigConfiguration() {
    return remoteConfig;
  }

  public FeatureFlagConfiguration getFeatureFlagConfiguration() {
    return featureFlag;
  }
}
