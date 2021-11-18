/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import com.vdurmont.semver4j.Semver;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RateLimitChallengeOptionManager {

  private final DynamicRateLimiters rateLimiters;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public static final String OPTION_RECAPTCHA = "recaptcha";
  public static final String OPTION_PUSH_CHALLENGE = "pushChallenge";

  public RateLimitChallengeOptionManager(final DynamicRateLimiters rateLimiters,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this.rateLimiters = rateLimiters;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public boolean isClientBelowMinimumVersion(final String userAgent) {
    try {
      final UserAgent client = UserAgentUtil.parseUserAgentString(userAgent);
      final Optional<Semver> minimumClientVersion = dynamicConfigurationManager.getConfiguration()
          .getRateLimitChallengeConfiguration()
          .getMinimumSupportedVersion(client.getPlatform());

      return minimumClientVersion.map(version -> version.isGreaterThan(client.getVersion()))
          .orElse(true);
    } catch (final UnrecognizedUserAgentException ignored) {
      return false;
    }
  }

  public List<String> getChallengeOptions(final Account account) {
    final List<String> options = new ArrayList<>(2);

    if (rateLimiters.getRecaptchaChallengeAttemptLimiter().hasAvailablePermits(account.getUuid(), 1) &&
        rateLimiters.getRecaptchaChallengeSuccessLimiter().hasAvailablePermits(account.getUuid(), 1)) {

      options.add(OPTION_RECAPTCHA);
    }

    if (rateLimiters.getPushChallengeAttemptLimiter().hasAvailablePermits(account.getUuid(), 1) &&
        rateLimiters.getPushChallengeSuccessLimiter().hasAvailablePermits(account.getUuid(), 1)) {

      options.add(OPTION_PUSH_CHALLENGE);
    }

    return options;
  }
}
