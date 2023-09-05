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
import java.util.Optional;
import java.util.Set;

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
  private final Set<String> testDevices;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;


  public RegistrationCaptchaManager(final CaptchaChecker captchaChecker, final RateLimiters rateLimiters,
      final Set<String> testDevices,
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
        ? Optional.of(captchaChecker.verify(Action.REGISTRATION, captcha.get(), sourceHost))
        : Optional.empty();
  }
}
