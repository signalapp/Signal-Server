/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceClient;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.recaptchaenterprise.v1.Assessment;
import com.google.recaptchaenterprise.v1.Event;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.ws.rs.BadRequestException;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RecaptchaClient {

  @VisibleForTesting
  static final String SEPARATOR = ".";
  @VisibleForTesting
  static final String V2_PREFIX = "signal-recaptcha-v2" + RecaptchaClient.SEPARATOR;
  private static final String ASSESSMENTS_COUNTER_NAME = name(RecaptchaClient.class, "assessments");
  private static final String SCORE_DISTRIBUTION_NAME = name(RecaptchaClient.class, "scoreDistribution");

  private final String projectPath;
  private final RecaptchaEnterpriseServiceClient client;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public RecaptchaClient(
      @Nonnull final String projectPath,
      @Nonnull final String recaptchaCredentialConfigurationJson,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    try {
      this.projectPath = Objects.requireNonNull(projectPath);
      this.client = RecaptchaEnterpriseServiceClient.create(RecaptchaEnterpriseServiceSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(
              new ByteArrayInputStream(recaptchaCredentialConfigurationJson.getBytes(StandardCharsets.UTF_8)))))
          .build());

      this.dynamicConfigurationManager = dynamicConfigurationManager;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Parses the sitekey, token, and action (if any) from {@code input}. The expected input format is: {@code [version
   * prefix.]sitekey.[action.]token}.
   * <p>
   * For action to be optional, there is a strong assumption that the token will never contain a {@value  SEPARATOR}.
   * Observation suggests {@code token} is base-64 encoded. In practice, an action should always be present, but we
   * don’t need to be strict.
   */
  static String[] parseInputToken(final String input) {
    String[] parts = StringUtils.removeStart(input, V2_PREFIX).split("\\" + SEPARATOR, 3);

    if (parts.length == 1) {
      throw new BadRequestException("too few parts");
    }

    if (parts.length == 2) {
      // we got some parts, assume it is action that is missing
      return new String[]{parts[0], null, parts[1]};
    }

    return parts;
  }

  public boolean verify(final String input, final String ip) {
    final String[] parts = parseInputToken(input);

    final String sitekey = parts[0];
    final String expectedAction = parts[1];
    final String token = parts[2];

    Event.Builder eventBuilder = Event.newBuilder()
        .setSiteKey(sitekey)
        .setToken(token)
        .setUserIpAddress(ip);

    if (expectedAction != null) {
      eventBuilder.setExpectedAction(expectedAction);
    }

    final Event event = eventBuilder.build();
    final Assessment assessment = client.createAssessment(projectPath, Assessment.newBuilder().setEvent(event).build());

    Metrics.counter(ASSESSMENTS_COUNTER_NAME,
            "action", String.valueOf(expectedAction),
            "valid", String.valueOf(assessment.getTokenProperties().getValid()))
        .increment();

    if (assessment.getTokenProperties().getValid()) {
      final float score = assessment.getRiskAnalysis().getScore();

      final DistributionSummary.Builder distributionSummaryBuilder = DistributionSummary.builder(
              SCORE_DISTRIBUTION_NAME)
          // score is 0.0…1.0, which doesn’t play well with distribution summary bucketing, so scale to 0…100
          .scale(100)
          .maximumExpectedValue(100.0d)
          .tags("action", String.valueOf(expectedAction));

      distributionSummaryBuilder.register(Metrics.globalRegistry).record(score);

      return score >= dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration().getScoreFloor()
          .floatValue();

    } else {
      return false;
    }
  }
}
