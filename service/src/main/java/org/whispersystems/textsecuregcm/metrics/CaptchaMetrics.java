/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;

public class CaptchaMetrics {

  private static final String CAPTCHA_OUTCOME_DISTRIBUTION_NAME = MetricsUtil.name(CaptchaMetrics.class, "outcome");

  private CaptchaMetrics() {}

  public static void measureCaptchaOutcome(final int score,
      final boolean success,
      final String region,
      final String context) {

    DistributionSummary.builder(CAPTCHA_OUTCOME_DISTRIBUTION_NAME)
        .tags("success", String.valueOf(success),
            "regionCode", region,
            "context", context)
        .minimumExpectedValue(1.0d)
        .maximumExpectedValue(100.0d)
        .serviceLevelObjectives(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
        .register(Metrics.globalRegistry)
        .record(Math.max(1, score));
  }
}
