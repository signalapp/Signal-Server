/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.CardinalityRateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class DynamicRateLimiters {

  private final FaultTolerantRedisCluster cacheCluster;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final AtomicReference<CardinalityRateLimiter> unsealedSenderCardinalityLimiter;
  private final AtomicReference<RateLimiter> unsealedIpLimiter;
  private final AtomicReference<RateLimiter> rateLimitResetLimiter;
  private final AtomicReference<RateLimiter> recaptchaChallengeAttemptLimiter;
  private final AtomicReference<RateLimiter> recaptchaChallengeSuccessLimiter;
  private final AtomicReference<RateLimiter> pushChallengeAttemptLimiter;
  private final AtomicReference<RateLimiter> pushChallengeSuccessLimiter;
  private final AtomicReference<RateLimiter> dailyPreKeysLimiter;

  public DynamicRateLimiters(final FaultTolerantRedisCluster rateLimitCluster,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this.cacheCluster = rateLimitCluster;
    this.dynamicConfigurationManager = dynamicConfigurationManager;

    this.dailyPreKeysLimiter = new AtomicReference<>(
        createDailyPreKeysLimiter(this.cacheCluster,
            this.dynamicConfigurationManager.getConfiguration().getLimits().getDailyPreKeys()));

    this.unsealedSenderCardinalityLimiter = new AtomicReference<>(createUnsealedSenderCardinalityLimiter(
        this.cacheCluster,
        this.dynamicConfigurationManager.getConfiguration().getLimits().getUnsealedSenderNumber()));

    this.unsealedIpLimiter = new AtomicReference<>(
        createUnsealedIpLimiter(this.cacheCluster,
            this.dynamicConfigurationManager.getConfiguration().getLimits().getUnsealedSenderIp()));

    this.rateLimitResetLimiter = new AtomicReference<>(
        createRateLimitResetLimiter(this.cacheCluster,
            this.dynamicConfigurationManager.getConfiguration().getLimits().getRateLimitReset()));

    this.recaptchaChallengeAttemptLimiter = new AtomicReference<>(createRecaptchaChallengeAttemptLimiter(
        this.cacheCluster,
        this.dynamicConfigurationManager.getConfiguration().getLimits().getRecaptchaChallengeAttempt()));

    this.recaptchaChallengeSuccessLimiter = new AtomicReference<>(createRecaptchaChallengeSuccessLimiter(
        this.cacheCluster,
        this.dynamicConfigurationManager.getConfiguration().getLimits().getRecaptchaChallengeSuccess()));

    this.pushChallengeAttemptLimiter = new AtomicReference<>(createPushChallengeAttemptLimiter(this.cacheCluster,
        this.dynamicConfigurationManager.getConfiguration().getLimits().getPushChallengeAttempt()));

    this.pushChallengeSuccessLimiter = new AtomicReference<>(createPushChallengeSuccessLimiter(this.cacheCluster,
        this.dynamicConfigurationManager.getConfiguration().getLimits().getPushChallengeSuccess()));
  }

  public CardinalityRateLimiter getUnsealedSenderCardinalityLimiter() {
    CardinalityRateLimitConfiguration currentConfiguration = dynamicConfigurationManager.getConfiguration().getLimits()
        .getUnsealedSenderNumber();

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
        dynamicConfigurationManager.getConfiguration().getLimits().getUnsealedSenderIp(),
        this::createUnsealedIpLimiter);
  }

  public RateLimiter getRateLimitResetLimiter() {
    return updateAndGetRateLimiter(
        rateLimitResetLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getRateLimitReset(),
        this::createRateLimitResetLimiter);
  }

  public RateLimiter getRecaptchaChallengeAttemptLimiter() {
    return updateAndGetRateLimiter(
        recaptchaChallengeAttemptLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getRecaptchaChallengeAttempt(),
        this::createRecaptchaChallengeAttemptLimiter);
  }

  public RateLimiter getRecaptchaChallengeSuccessLimiter() {
    return updateAndGetRateLimiter(
        recaptchaChallengeSuccessLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getRecaptchaChallengeSuccess(),
        this::createRecaptchaChallengeSuccessLimiter);
  }

  public RateLimiter getPushChallengeAttemptLimiter() {
    return updateAndGetRateLimiter(
        pushChallengeAttemptLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getPushChallengeAttempt(),
        this::createPushChallengeAttemptLimiter);
  }

  public RateLimiter getPushChallengeSuccessLimiter() {
    return updateAndGetRateLimiter(
        pushChallengeSuccessLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getPushChallengeSuccess(),
        this::createPushChallengeSuccessLimiter);
  }

  public RateLimiter getDailyPreKeysLimiter() {
    return updateAndGetRateLimiter(
        dailyPreKeysLimiter,
        dynamicConfigurationManager.getConfiguration().getLimits().getDailyPreKeys(),
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

  private CardinalityRateLimiter createUnsealedSenderCardinalityLimiter(FaultTolerantRedisCluster cacheCluster,
      CardinalityRateLimitConfiguration configuration) {
    return new CardinalityRateLimiter(cacheCluster, "unsealedSender", configuration.getTtl(),
        configuration.getMaxCardinality());
  }

  private RateLimiter createUnsealedIpLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "unsealedIp");
  }

  public RateLimiter createRateLimitResetLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "rateLimitReset");
  }

  public RateLimiter createRecaptchaChallengeAttemptLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "recaptchaChallengeAttempt");
  }

  public RateLimiter createRecaptchaChallengeSuccessLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "recaptchaChallengeSuccess");
  }

  public RateLimiter createPushChallengeAttemptLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "pushChallengeAttempt");
  }

  public RateLimiter createPushChallengeSuccessLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "pushChallengeSuccess");
  }

  public RateLimiter createDailyPreKeysLimiter(FaultTolerantRedisCluster cacheCluster,
      RateLimitConfiguration configuration) {
    return createLimiter(cacheCluster, configuration, "dailyPreKeys");
  }

  private RateLimiter createLimiter(FaultTolerantRedisCluster cacheCluster, RateLimitConfiguration configuration,
      String name) {
    return new RateLimiter(cacheCluster, name,
        configuration.getBucketSize(),
        configuration.getLeakRatePerMinute());
  }
}
