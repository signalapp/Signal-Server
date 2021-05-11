/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RateLimiters {

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter voiceDestinationDailyLimiter;
  private final RateLimiter smsVoiceIpLimiter;
  private final RateLimiter smsVoicePrefixLimiter;
  private final RateLimiter autoBlockLimiter;
  private final RateLimiter verifyLimiter;
  private final RateLimiter pinLimiter;

  private final RateLimiter attachmentLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;

  private final RateLimiter allocateDeviceLimiter;
  private final RateLimiter verifyDeviceLimiter;

  private final RateLimiter turnLimiter;

  private final RateLimiter profileLimiter;
  private final RateLimiter stickerPackLimiter;
  private final RateLimiter usernameLookupLimiter;
  private final RateLimiter usernameSetLimiter;

  private final AtomicReference<CardinalityRateLimiter> unsealedSenderCardinalityLimiter;
  private final AtomicReference<RateLimiter> unsealedIpLimiter;
  private final AtomicReference<RateLimiter> rateLimitResetLimiter;
  private final AtomicReference<RateLimiter> recaptchaChallengeAttemptLimiter;
  private final AtomicReference<RateLimiter> recaptchaChallengeSuccessLimiter;
  private final AtomicReference<RateLimiter> pushChallengeAttemptLimiter;
  private final AtomicReference<RateLimiter> pushChallengeSuccessLimiter;
  private final AtomicReference<RateLimiter> dailyPreKeysLimiter;

  private final FaultTolerantRedisCluster   cacheCluster;
  private final DynamicConfigurationManager dynamicConfig;

  public RateLimiters(RateLimitsConfiguration config, DynamicConfigurationManager dynamicConfig, FaultTolerantRedisCluster cacheCluster) {
    this.cacheCluster  = cacheCluster;
    this.dynamicConfig = dynamicConfig;

    this.smsDestinationLimiter = new RateLimiter(cacheCluster, "smsDestination",
                                                 config.getSmsDestination().getBucketSize(),
                                                 config.getSmsDestination().getLeakRatePerMinute());

    this.voiceDestinationLimiter = new RateLimiter(cacheCluster, "voxDestination",
                                                   config.getVoiceDestination().getBucketSize(),
                                                   config.getVoiceDestination().getLeakRatePerMinute());

    this.voiceDestinationDailyLimiter = new RateLimiter(cacheCluster, "voxDestinationDaily",
                                                        config.getVoiceDestinationDaily().getBucketSize(),
                                                        config.getVoiceDestinationDaily().getLeakRatePerMinute());

    this.smsVoiceIpLimiter = new RateLimiter(cacheCluster, "smsVoiceIp",
                                             config.getSmsVoiceIp().getBucketSize(),
                                             config.getSmsVoiceIp().getLeakRatePerMinute());

    this.smsVoicePrefixLimiter = new RateLimiter(cacheCluster, "smsVoicePrefix",
                                                 config.getSmsVoicePrefix().getBucketSize(),
                                                 config.getSmsVoicePrefix().getLeakRatePerMinute());

    this.autoBlockLimiter = new RateLimiter(cacheCluster, "autoBlock",
                                            config.getAutoBlock().getBucketSize(),
                                            config.getAutoBlock().getLeakRatePerMinute());

    this.verifyLimiter = new LockingRateLimiter(cacheCluster, "verify",
                                                config.getVerifyNumber().getBucketSize(),
                                                config.getVerifyNumber().getLeakRatePerMinute());

    this.pinLimiter = new LockingRateLimiter(cacheCluster, "pin",
                                             config.getVerifyPin().getBucketSize(),
                                             config.getVerifyPin().getLeakRatePerMinute());

    this.attachmentLimiter = new RateLimiter(cacheCluster, "attachmentCreate",
                                             config.getAttachments().getBucketSize(),
                                             config.getAttachments().getLeakRatePerMinute());

    this.preKeysLimiter = new RateLimiter(cacheCluster, "prekeys",
                                          config.getPreKeys().getBucketSize(),
                                          config.getPreKeys().getLeakRatePerMinute());

    this.messagesLimiter = new RateLimiter(cacheCluster, "messages",
                                           config.getMessages().getBucketSize(),
                                           config.getMessages().getLeakRatePerMinute());

    this.allocateDeviceLimiter = new RateLimiter(cacheCluster, "allocateDevice",
                                                 config.getAllocateDevice().getBucketSize(),
                                                 config.getAllocateDevice().getLeakRatePerMinute());

    this.verifyDeviceLimiter = new RateLimiter(cacheCluster, "verifyDevice",
                                               config.getVerifyDevice().getBucketSize(),
                                               config.getVerifyDevice().getLeakRatePerMinute());

    this.turnLimiter = new RateLimiter(cacheCluster, "turnAllocate",
                                       config.getTurnAllocations().getBucketSize(),
                                       config.getTurnAllocations().getLeakRatePerMinute());

    this.profileLimiter = new RateLimiter(cacheCluster, "profile",
                                          config.getProfile().getBucketSize(),
                                          config.getProfile().getLeakRatePerMinute());

    this.stickerPackLimiter = new RateLimiter(cacheCluster, "stickerPack",
                                              config.getStickerPack().getBucketSize(),
                                              config.getStickerPack().getLeakRatePerMinute());

    this.usernameLookupLimiter = new RateLimiter(cacheCluster, "usernameLookup",
                                                 config.getUsernameLookup().getBucketSize(),
                                                 config.getUsernameLookup().getLeakRatePerMinute());

    this.usernameSetLimiter = new RateLimiter(cacheCluster, "usernameSet",
                                              config.getUsernameSet().getBucketSize(),
                                              config.getUsernameSet().getLeakRatePerMinute());

    this.dailyPreKeysLimiter = new AtomicReference<>(createDailyPreKeysLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getDailyPreKeys()));

    this.unsealedSenderCardinalityLimiter = new AtomicReference<>(createUnsealedSenderCardinalityLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getUnsealedSenderNumber()));
    this.unsealedIpLimiter     = new AtomicReference<>(createUnsealedIpLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp()));

    this.rateLimitResetLimiter = new AtomicReference<>(
        createRateLimitResetLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getRateLimitReset()));

    this.recaptchaChallengeAttemptLimiter = new AtomicReference<>(createRecaptchaChallengeAttemptLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getRecaptchaChallengeAttempt()));
    this.recaptchaChallengeSuccessLimiter = new AtomicReference<>(createRecaptchaChallengeSuccessLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getRecaptchaChallengeSuccess()));
    this.pushChallengeAttemptLimiter = new AtomicReference<>(createPushChallengeAttemptLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getPushChallengeAttempt()));
    this.pushChallengeSuccessLimiter = new AtomicReference<>(createPushChallengeSuccessLimiter(cacheCluster, dynamicConfig.getConfiguration().getLimits().getPushChallengeSuccess()));
  }

  public CardinalityRateLimiter getUnsealedSenderCardinalityLimiter() {
    CardinalityRateLimitConfiguration currentConfiguration = dynamicConfig.getConfiguration().getLimits().getUnsealedSenderNumber();

    return this.unsealedSenderCardinalityLimiter.updateAndGet(rateLimiter -> {
      if (rateLimiter.hasConfiguration(currentConfiguration)) {
        return rateLimiter;
      } else {
        return createUnsealedSenderCardinalityLimiter(cacheCluster, currentConfiguration);
      }
    });
  }

  public RateLimiter getUnsealedIpLimiter() {
    return updateAndGetRateLimiter(
        unsealedIpLimiter,
        dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp(),
        this::createUnsealedIpLimiter);
  }

  public RateLimiter getRateLimitResetLimiter() {
    return updateAndGetRateLimiter(
        rateLimitResetLimiter,
        dynamicConfig.getConfiguration().getLimits().getRateLimitReset(),
        this::createRateLimitResetLimiter);
  }

  public RateLimiter getRecaptchaChallengeAttemptLimiter() {
    return updateAndGetRateLimiter(
        recaptchaChallengeAttemptLimiter,
        dynamicConfig.getConfiguration().getLimits().getRecaptchaChallengeAttempt(),
        this::createRecaptchaChallengeAttemptLimiter);
  }

  public RateLimiter getRecaptchaChallengeSuccessLimiter() {
    return updateAndGetRateLimiter(
        recaptchaChallengeSuccessLimiter,
        dynamicConfig.getConfiguration().getLimits().getRecaptchaChallengeSuccess(),
        this::createRecaptchaChallengeSuccessLimiter);
  }

  public RateLimiter getPushChallengeAttemptLimiter() {
    return updateAndGetRateLimiter(
        pushChallengeAttemptLimiter,
        dynamicConfig.getConfiguration().getLimits().getPushChallengeAttempt(),
        this::createPushChallengeAttemptLimiter);
  }

  public RateLimiter getPushChallengeSuccessLimiter() {
    return updateAndGetRateLimiter(
        pushChallengeSuccessLimiter,
        dynamicConfig.getConfiguration().getLimits().getPushChallengeSuccess(),
        this::createPushChallengeSuccessLimiter);
  }

  public RateLimiter getDailyPreKeysLimiter() {
    return updateAndGetRateLimiter(
        dailyPreKeysLimiter,
        dynamicConfig.getConfiguration().getLimits().getDailyPreKeys(),
        this::createDailyPreKeysLimiter);
  }

  private RateLimiter updateAndGetRateLimiter(final AtomicReference<RateLimiter> rateLimiter,
      RateLimitConfiguration currentConfiguration,
      BiFunction<FaultTolerantRedisCluster, RateLimitConfiguration, RateLimiter> rateLimitFactory) {

    return rateLimiter.updateAndGet(limiter -> {
      if (limiter.hasConfiguration(currentConfiguration)) {
        return limiter;
      } else {
        return rateLimitFactory.apply(cacheCluster, currentConfiguration);
      }
    });
  }

  public RateLimiter getAllocateDeviceLimiter() {
    return allocateDeviceLimiter;
  }

  public RateLimiter getVerifyDeviceLimiter() {
    return verifyDeviceLimiter;
  }

  public RateLimiter getMessagesLimiter() {
    return messagesLimiter;
  }

  public RateLimiter getPreKeysLimiter() {
    return preKeysLimiter;
  }

  public RateLimiter getAttachmentLimiter() {
    return this.attachmentLimiter;
  }

  public RateLimiter getSmsDestinationLimiter() {
    return smsDestinationLimiter;
  }

  public RateLimiter getSmsVoiceIpLimiter() {
    return smsVoiceIpLimiter;
  }

  public RateLimiter getSmsVoicePrefixLimiter() {
    return smsVoicePrefixLimiter;
  }

  public RateLimiter getAutoBlockLimiter() {
    return autoBlockLimiter;
  }

  public RateLimiter getVoiceDestinationLimiter() {
    return voiceDestinationLimiter;
  }

  public RateLimiter getVoiceDestinationDailyLimiter() {
    return voiceDestinationDailyLimiter;
  }

  public RateLimiter getVerifyLimiter() {
    return verifyLimiter;
  }

  public RateLimiter getPinLimiter() {
    return pinLimiter;
  }

  public RateLimiter getTurnLimiter() {
    return turnLimiter;
  }

  public RateLimiter getProfileLimiter() {
    return profileLimiter;
  }

  public RateLimiter getStickerPackLimiter() {
    return stickerPackLimiter;
  }

  public RateLimiter getUsernameLookupLimiter() {
    return usernameLookupLimiter;
  }

  public RateLimiter getUsernameSetLimiter() {
    return usernameSetLimiter;
  }

  private CardinalityRateLimiter createUnsealedSenderCardinalityLimiter(FaultTolerantRedisCluster cacheCluster, CardinalityRateLimitConfiguration configuration) {
    return new CardinalityRateLimiter(cacheCluster, "unsealedSender", configuration.getTtl(), configuration.getMaxCardinality());
  }

  private RateLimiter createUnsealedIpLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration)
  {
    return createLimiter(cacheCluster, configuration, "unsealedIp");
  }

  public RateLimiter createRateLimitResetLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "rateLimitReset");
  }

  public RateLimiter createRecaptchaChallengeAttemptLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "recaptchaChallengeAttempt");
  }

  public RateLimiter createRecaptchaChallengeSuccessLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "recaptchaChallengeSuccess");
  }

  public RateLimiter createPushChallengeAttemptLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "pushChallengeAttempt");
  }

  public RateLimiter createPushChallengeSuccessLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "pushChallengeSuccess");
  }

  public RateLimiter createDailyPreKeysLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "dailyPreKeys");
  }

  private RateLimiter createLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration, String name) {
    return new RateLimiter(cacheCluster, name,
                           configuration.getBucketSize(),
                           configuration.getLeakRatePerMinute());
  }
}
