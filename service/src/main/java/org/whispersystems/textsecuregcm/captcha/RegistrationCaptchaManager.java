/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

public class RegistrationCaptchaManager {

  private static final Logger logger = LoggerFactory.getLogger(RegistrationCaptchaManager.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter countryFilteredHostMeter = metricRegistry.meter(
      name(AccountController.class, "country_limited_host"));
  private final Meter rateLimitedHostMeter = metricRegistry.meter(name(AccountController.class, "rate_limited_host"));
  private final Meter rateLimitedPrefixMeter = metricRegistry.meter(
      name(AccountController.class, "rate_limited_prefix"));

  private final CaptchaChecker captchaChecker;
  private final RateLimiters rateLimiters;
  private final Map<String, Integer> testDevices;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;


  public RegistrationCaptchaManager(final CaptchaChecker captchaChecker, final RateLimiters rateLimiters,
      final Map<String, Integer> testDevices,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.captchaChecker = captchaChecker;
    this.rateLimiters = rateLimiters;
    this.testDevices = testDevices;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public Optional<AssessmentResult> assessCaptcha(final Optional<String> captcha, final String sourceHost)
      throws IOException {
    return captcha.isPresent()
        ? Optional.of(captchaChecker.verify(captcha.get(), sourceHost))
        : Optional.empty();
  }

  public boolean requiresCaptcha(final String number, final String forwardedFor, String sourceHost,
      final boolean pushChallengeMatch) {
    if (testDevices.containsKey(number)) {
      return false;
    }

    if (!pushChallengeMatch) {
      return true;
    }

    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);

    DynamicCaptchaConfiguration captchaConfig = dynamicConfigurationManager.getConfiguration()
        .getCaptchaConfiguration();

    boolean countryFiltered = captchaConfig.getSignupCountryCodes().contains(countryCode) ||
        captchaConfig.getSignupRegions().contains(region);

    try {
      rateLimiters.getSmsVoiceIpLimiter().validate(sourceHost);
    } catch (RateLimitExceededException e) {
      logger.info("Rate limit exceeded: {}, {} ({})", number, sourceHost, forwardedFor);
      rateLimitedHostMeter.mark();

      return true;
    }

    try {
      rateLimiters.getSmsVoicePrefixLimiter().validate(Util.getNumberPrefix(number));
    } catch (RateLimitExceededException e) {
      logger.info("Prefix rate limit exceeded: {}, {} ({})", number, sourceHost, forwardedFor);
      rateLimitedPrefixMeter.mark();

      return true;
    }

    if (countryFiltered) {
      countryFilteredHostMeter.mark();
      return true;
    }

    return false;
  }

}
