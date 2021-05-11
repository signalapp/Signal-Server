/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.util.Duration;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Util;

public class PreKeyRateLimiter {

  private static final String RATE_LIMIT_RESET_COUNTER_NAME = name(PreKeyRateLimiter.class, "reset");
  private static final String RATE_LIMITED_PREKEYS_COUNTER_NAME = name(PreKeyRateLimiter.class, "rateLimited");
  private static final String RATE_LIMITED_PREKEYS_TOTAL_ACCOUNTS_COUNTER_NAME = name(PreKeyRateLimiter.class, "rateLimited");
  private static final String RATE_LIMITED_PREKEYS_ACCOUNTS_ENFORCED_COUNTER_NAME = name(PreKeyRateLimiter.class, "rateLimitedAccountsEnforced");
  private static final String RATE_LIMITED_PREKEYS_ACCOUNTS_UNENFORCED_COUNTER_NAME = name(PreKeyRateLimiter.class, "rateLimitedAccountsUnenforced");

  private static final String RATE_LIMITED_ACCOUNTS_HLL_KEY = "PreKeyRateLimiter::rateLimitedAccounts";
  private static final String RATE_LIMITED_ACCOUNTS_ENFORCED_HLL_KEY = "PreKeyRateLimiter::rateLimitedAccounts::enforced";
  private static final String RATE_LIMITED_ACCOUNTS_UNENFORCED_HLL_KEY = "PreKeyRateLimiter::rateLimitedAccounts::unenforced";
  private static final long RATE_LIMITED_ACCOUNTS_HLL_TTL_SECONDS = Duration.days(1).toSeconds();

  private final RateLimiters rateLimiters;
  private final DynamicConfigurationManager dynamicConfigurationManager;
  private final RateLimitResetMetricsManager metricsManager;

  public PreKeyRateLimiter(final RateLimiters rateLimiters,
      final DynamicConfigurationManager dynamicConfigurationManager,
      final RateLimitResetMetricsManager metricsManager) {
    this.rateLimiters = rateLimiters;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.metricsManager = metricsManager;

    metricsManager.initializeFunctionCounters(RATE_LIMITED_PREKEYS_TOTAL_ACCOUNTS_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_HLL_KEY);
    metricsManager.initializeFunctionCounters(RATE_LIMITED_PREKEYS_ACCOUNTS_ENFORCED_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_ENFORCED_HLL_KEY);
    metricsManager.initializeFunctionCounters(RATE_LIMITED_PREKEYS_ACCOUNTS_UNENFORCED_COUNTER_NAME,
        RATE_LIMITED_ACCOUNTS_UNENFORCED_HLL_KEY);
  }

  public void validate(final Account account) throws RateLimitExceededException {

    try {
      rateLimiters.getDailyPreKeysLimiter().validate(account.getNumber());
    } catch (final RateLimitExceededException e) {

      final boolean enforceLimit = dynamicConfigurationManager.getConfiguration()
          .getRateLimitChallengeConfiguration().isPreKeyLimitEnforced();

      metricsManager.recordMetrics(account, enforceLimit,
          RATE_LIMITED_PREKEYS_COUNTER_NAME,
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

    rateLimiters.getDailyPreKeysLimiter().clear(account.getNumber());

    Metrics.counter(RATE_LIMIT_RESET_COUNTER_NAME, "countryCode", Util.getCountryCode(account.getNumber()))
        .increment();
  }
}
