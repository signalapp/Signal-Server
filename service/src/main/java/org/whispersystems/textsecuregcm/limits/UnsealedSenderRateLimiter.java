/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.util.Duration;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitsConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Util;

public class UnsealedSenderRateLimiter {

  private final RateLimiters rateLimiters;
  private final FaultTolerantRedisCluster rateLimitCluster;
  private final DynamicConfigurationManager dynamicConfigurationManager;
  private final RateLimitResetMetricsManager metricsManager;

  private static final String RATE_LIMIT_RESET_COUNTER_NAME = name(UnsealedSenderRateLimiter.class, "reset");
  private static final String RATE_LIMITED_UNSEALED_SENDER_COUNTER_NAME = name(UnsealedSenderRateLimiter.class, "rateLimited");
  private static final String RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_TOTAL_COUNTER_NAME = name(UnsealedSenderRateLimiter.class, "rateLimitedAccountsTotal");
  private static final String RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_ENFORCED_COUNTER_NAME = name(UnsealedSenderRateLimiter.class, "rateLimitedAccountsEnforced");
  private static final String RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_UNENFORCED_COUNTER_NAME = name(UnsealedSenderRateLimiter.class, "rateLimitedAccountsUnenforced");

  private static final String RATE_LIMITED_ACCOUNTS_HLL_KEY = "UnsealedSenderRateLimiter::rateLimitedAccounts::total";
  private static final String RATE_LIMITED_ACCOUNTS_ENFORCED_HLL_KEY = "UnsealedSenderRateLimiter::rateLimitedAccounts::enforced";
  private static final String RATE_LIMITED_ACCOUNTS_UNENFORCED_HLL_KEY = "UnsealedSenderRateLimiter::rateLimitedAccounts::unenforced";
  private static final long RATE_LIMITED_ACCOUNTS_HLL_TTL_SECONDS = Duration.days(1).toSeconds();


  public UnsealedSenderRateLimiter(final RateLimiters rateLimiters,
      final FaultTolerantRedisCluster rateLimitCluster,
      final DynamicConfigurationManager dynamicConfigurationManager,
      final RateLimitResetMetricsManager metricsManager) {

    this.rateLimiters = rateLimiters;
    this.rateLimitCluster = rateLimitCluster;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.metricsManager = metricsManager;

    metricsManager.initializeFunctionCounters(RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_TOTAL_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_HLL_KEY);
    metricsManager.initializeFunctionCounters(RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_ENFORCED_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_ENFORCED_HLL_KEY);
    metricsManager.initializeFunctionCounters(RATE_LIMITED_UNSEALED_SENDER_ACCOUNTS_UNENFORCED_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_UNENFORCED_HLL_KEY);
  }

  public void validate(final Account sender, final Account destination) throws RateLimitExceededException {
    final int maxCardinality = rateLimitCluster.withCluster(connection -> {
      final String cardinalityString = connection.sync().get(getMaxCardinalityKey(sender));

      return cardinalityString != null
          ? Integer.parseInt(cardinalityString)
          : dynamicConfigurationManager.getConfiguration().getLimits().getUnsealedSenderDefaultCardinalityLimit();
    });

    try {
      rateLimiters.getUnsealedSenderCardinalityLimiter()
          .validate(sender.getNumber(), destination.getUuid().toString(), maxCardinality);
    } catch (final RateLimitExceededException e) {

      final boolean enforceLimit = dynamicConfigurationManager.getConfiguration()
          .getRateLimitChallengeConfiguration().isUnsealedSenderLimitEnforced();

      metricsManager.recordMetrics(sender, enforceLimit, RATE_LIMITED_UNSEALED_SENDER_COUNTER_NAME,
          enforceLimit ? RATE_LIMITED_ACCOUNTS_ENFORCED_HLL_KEY : RATE_LIMITED_ACCOUNTS_UNENFORCED_HLL_KEY,
          RATE_LIMITED_ACCOUNTS_HLL_KEY,
          RATE_LIMITED_ACCOUNTS_HLL_TTL_SECONDS
          );

      if (enforceLimit) {
        throw e;
      }
    }
  }

  public void handleRateLimitReset(final Account account) {
    rateLimitCluster.useCluster(connection -> {
      final CardinalityRateLimiter unsealedSenderCardinalityLimiter = rateLimiters.getUnsealedSenderCardinalityLimiter();
      final DynamicRateLimitsConfiguration rateLimitsConfiguration =
          dynamicConfigurationManager.getConfiguration().getLimits();

      final long ttl;
      {
        final long remainingTtl = unsealedSenderCardinalityLimiter.getRemainingTtl(account.getNumber());
        ttl = remainingTtl > 0 ? remainingTtl : unsealedSenderCardinalityLimiter.getInitialTtl().toSeconds();
      }

      final String key = getMaxCardinalityKey(account);

      connection.sync().set(key,
          String.valueOf(rateLimitsConfiguration.getUnsealedSenderDefaultCardinalityLimit()),
          SetArgs.Builder.nx().ex(ttl));

      connection.sync().incrby(key, rateLimitsConfiguration.getUnsealedSenderPermitIncrement());
    });

    Metrics.counter(RATE_LIMIT_RESET_COUNTER_NAME,
        "countryCode", Util.getCountryCode(account.getNumber())).increment();
  }

  private static String getMaxCardinalityKey(final Account account) {
    return "max_unsealed_sender_cardinality::" + account.getUuid();
  }
}
