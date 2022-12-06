/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceClient;
import com.google.cloud.recaptchaenterprise.v1.RecaptchaEnterpriseServiceSettings;
import com.google.recaptchaenterprise.v1.Assessment;
import com.google.recaptchaenterprise.v1.Event;
import com.google.recaptchaenterprise.v1.RiskAnalysis;
import io.micrometer.core.instrument.Metrics;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RecaptchaClient implements CaptchaClient {

  private static final Logger log = LoggerFactory.getLogger(RecaptchaClient.class);

  private static final String V2_PREFIX = "signal-recaptcha-v2";
  private static final String INVALID_REASON_COUNTER_NAME = name(RecaptchaClient.class, "invalidReason");
  private static final String ASSESSMENT_REASON_COUNTER_NAME = name(RecaptchaClient.class, "assessmentReason");

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

  @Override
  public String scheme() {
    return V2_PREFIX;
  }

  @Override
  public org.whispersystems.textsecuregcm.captcha.AssessmentResult verify(final String sitekey,
      final @Nullable String expectedAction,
      final String token, final String ip) throws IOException {
    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    if (!config.isAllowRecaptcha()) {
      log.warn("Received request to verify a recaptcha, but recaptcha is not enabled");
      return AssessmentResult.invalid();
    }

    Event.Builder eventBuilder = Event.newBuilder()
        .setSiteKey(sitekey)
        .setToken(token)
        .setUserIpAddress(ip);

    if (expectedAction != null) {
      eventBuilder.setExpectedAction(expectedAction);
    }

    final Event event = eventBuilder.build();
    final Assessment assessment;
    try {
      assessment = client.createAssessment(projectPath, Assessment.newBuilder().setEvent(event).build());
    } catch (ApiException e) {
      throw new IOException(e);
    }

    if (assessment.getTokenProperties().getValid()) {
      final float score = assessment.getRiskAnalysis().getScore();
      log.debug("assessment for {} was valid, score: {}", expectedAction, score);
      for (RiskAnalysis.ClassificationReason reason : assessment.getRiskAnalysis().getReasonsList()) {
        Metrics.counter(ASSESSMENT_REASON_COUNTER_NAME,
                "action", String.valueOf(expectedAction),
                "score", AssessmentResult.scoreString(score),
                "reason", reason.name())
            .increment();
      }
      return new AssessmentResult(
          score >= config.getScoreFloor().floatValue(),
          AssessmentResult.scoreString(score));
    } else {
      Metrics.counter(INVALID_REASON_COUNTER_NAME,
              "action", String.valueOf(expectedAction),
              "reason", assessment.getTokenProperties().getInvalidReason().name())
          .increment();
      return AssessmentResult.invalid();
    }
  }
}
