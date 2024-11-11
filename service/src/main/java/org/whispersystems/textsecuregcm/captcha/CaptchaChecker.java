/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.BadRequestException;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CaptchaChecker {
  private static final Logger logger = LoggerFactory.getLogger(CaptchaChecker.class);
  private static final String INVALID_SITEKEY_COUNTER_NAME = name(CaptchaChecker.class, "invalidSiteKey");
  private static final String ASSESSMENTS_COUNTER_NAME = name(CaptchaChecker.class, "assessments");
  private static final String INVALID_ACTION_COUNTER_NAME = name(CaptchaChecker.class, "invalidActions");

  @VisibleForTesting
  static final String SEPARATOR = ".";

  private static final String SHORT_SUFFIX = "-short";

  private final ShortCodeExpander shortCodeExpander;
  private final Function<String, CaptchaClient> captchaClientSupplier;

  public CaptchaChecker(
      final ShortCodeExpander shortCodeRetriever,
      final Function<String, CaptchaClient> captchaClientSupplier) {
    this.shortCodeExpander = shortCodeRetriever;
    this.captchaClientSupplier = captchaClientSupplier;
  }


  /**
   * Check if a solved captcha should be accepted
   *
   * @param maybeAci       optional account UUID of the user solving the captcha
   * @param expectedAction the {@link Action} for which this captcha solution is intended
   * @param input          expected to contain a prefix indicating the captcha scheme, sitekey, token, and action. The
   *                       expected format is {@code version-prefix.sitekey.action.token}
   * @param ip             IP of the solver
   * @param userAgent      User-Agent of the solver
   * @return An {@link AssessmentResult} indicating whether the solution should be accepted, and a score that can be
   * used for metrics
   * @throws IOException         if there is an error validating the captcha with the underlying service
   * @throws BadRequestException if input is not in the expected format
   */
  public AssessmentResult verify(
      final Optional<UUID> maybeAci,
      final Action expectedAction,
      final String input,
      final String ip,
      final String userAgent) throws IOException {
    final String[] parts = input.split("\\" + SEPARATOR, 4);

    // we allow missing actions, if we're missing 1 part, assume it's the action
    if (parts.length < 4) {
      throw new BadRequestException("too few parts");
    }

    final String prefix = parts[0];
    final String siteKey = parts[1].toLowerCase(Locale.ROOT).strip();
    final String action = parts[2];
    String token = parts[3];

    String provider = prefix;
    if (prefix.endsWith(SHORT_SUFFIX)) {
      // This is a "short" solution that points to the actual solution. We need to fetch the
      // full solution before proceeding
      provider = prefix.substring(0, prefix.length() - SHORT_SUFFIX.length());
      token = shortCodeExpander.retrieve(token).orElseThrow(() -> new BadRequestException("invalid shortcode"));
    }

    final CaptchaClient client = this.captchaClientSupplier.apply(provider);
    if (client == null) {
      throw new BadRequestException("invalid captcha scheme");
    }

    final Action parsedAction = Action.parse(action)
        .orElseThrow(() -> {
          Metrics.counter(INVALID_ACTION_COUNTER_NAME, "action", action).increment();
          return new BadRequestException("invalid captcha action");
        });

    if (!parsedAction.equals(expectedAction)) {
      Metrics.counter(INVALID_ACTION_COUNTER_NAME, "action", action).increment();
      throw new BadRequestException("invalid captcha action");
    }

    final Set<String> allowedSiteKeys = client.validSiteKeys(parsedAction);
    if (!allowedSiteKeys.contains(siteKey)) {
      logger.debug("invalid site-key {}, action={}, token={}", siteKey, action, token);
      Metrics.counter(INVALID_SITEKEY_COUNTER_NAME, "action", action).increment();
      throw new BadRequestException("invalid captcha site-key");
    }

    final AssessmentResult result = client.verify(maybeAci, siteKey, parsedAction, token, ip, userAgent);
    Metrics.counter(ASSESSMENTS_COUNTER_NAME,
            "action", action,
            "score", result.getScoreString(),
            "provider", provider)
        .increment();
    return result;
  }
}
