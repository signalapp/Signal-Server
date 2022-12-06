/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class CaptchaChecker {
  private static final String ASSESSMENTS_COUNTER_NAME = name(RecaptchaClient.class, "assessments");

  @VisibleForTesting
  static final String SEPARATOR = ".";

  private final Map<String, CaptchaClient> captchaClientMap;

  public CaptchaChecker(final List<CaptchaClient> captchaClients) {
    this.captchaClientMap = captchaClients.stream()
        .collect(Collectors.toMap(CaptchaClient::scheme, Function.identity()));
  }

  /**
   * Check if a solved captcha should be accepted
   * <p>
   *
   * @param input expected to contain a prefix indicating the captcha scheme, sitekey, token, and action. The expected
   *              format is {@code version-prefix.sitekey.[action.]token}
   * @param ip    IP of the solver
   * @return An {@link AssessmentResult} indicating whether the solution should be accepted, and a score that can be
   * used for metrics
   * @throws IOException         if there is an error validating the captcha with the underlying service
   * @throws BadRequestException if input is not in the expected format
   */
  public AssessmentResult verify(final String input, final String ip) throws IOException {
    /*
     * For action to be optional, there is a strong assumption that the token will never contain a {@value  SEPARATOR}.
     * Observation suggests {@code token} is base-64 encoded. In practice, an action should always be present, but we
     * don’t need to be strict.
     */
    final String[] parts = input.split("\\" + SEPARATOR, 4);

    // we allow missing actions, if we're missing 1 part, assume it's the action
    if (parts.length < 3) {
      throw new BadRequestException("too few parts");
    }

    int idx = 0;
    final String prefix = parts[idx++];
    final String siteKey = parts[idx++];
    final String action = parts.length == 3 ? null : parts[idx++];
    final String token = parts[idx];

    final CaptchaClient client = this.captchaClientMap.get(prefix);
    if (client == null) {
      throw new BadRequestException("invalid captcha scheme");
    }
    final AssessmentResult result = client.verify(siteKey, action, token, ip);
    Metrics.counter(ASSESSMENTS_COUNTER_NAME,
            "action", String.valueOf(action),
            "valid", String.valueOf(result.valid()),
            "score", result.score(),
            "provider", prefix)
        .increment();
    return result;
  }
}
