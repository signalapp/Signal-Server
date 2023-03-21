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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RecaptchaClient implements CaptchaClient {

  private static final Logger log = LoggerFactory.getLogger(RecaptchaClient.class);

  private static final String V2_PREFIX = "signal-recaptcha-v2";
  private static final String INVALID_REASON_COUNTER_NAME = name(RecaptchaClient.class, "invalidReason");
  private static final String INVALID_SITEKEY_COUNTER_NAME = name(RecaptchaClient.class, "invalidSiteKey");
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
  public Set<String> validSiteKeys(final Action action) {
    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    if (!config.isAllowRecaptcha()) {
      log.warn("Received request to verify a recaptcha, but recaptcha is not enabled");
      return Collections.emptySet();
    }
    return Optional
        .ofNullable(config.getRecaptchaSiteKeys().get(action))
        .orElse(Collections.emptySet());
  }

  @Override
  public org.whispersystems.textsecuregcm.captcha.AssessmentResult verify(
      final String sitekey,
      final Action action,
      final String token,
      final String ip) throws IOException {
    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    final Set<String> allowedSiteKeys = config.getRecaptchaSiteKeys().get(action);
    if (allowedSiteKeys != null && !allowedSiteKeys.contains(sitekey)) {
      log.info("invalid recaptcha sitekey {}, action={}, token={}", action, token);
      Metrics.counter(INVALID_SITEKEY_COUNTER_NAME, "action", action.getActionName()).increment();
      return AssessmentResult.invalid();
    }

    Event.Builder eventBuilder = Event.newBuilder()
        .setSiteKey(sitekey)
        .setToken(token)
        .setUserIpAddress(ip);

    if (action != null) {
      eventBuilder.setExpectedAction(action.getActionName());
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
      log.debug("assessment for {} was valid, score: {}", action.getActionName(), score);
      final BigDecimal threshold = config.getScoreFloorByAction().getOrDefault(action, config.getScoreFloor());
      final AssessmentResult assessmentResult = AssessmentResult.fromScore(score, threshold.floatValue());
      for (RiskAnalysis.ClassificationReason reason : assessment.getRiskAnalysis().getReasonsList()) {
        Metrics.counter(ASSESSMENT_REASON_COUNTER_NAME,
                "action", action.getActionName(),
                "score", assessmentResult.getScoreString(),
                "reason", reason.name())
            .increment();
      }
      return assessmentResult;
    } else {
      Metrics.counter(INVALID_REASON_COUNTER_NAME,
              "action", action.getActionName(),
              "reason", assessment.getTokenProperties().getInvalidReason().name())
          .increment();
      return AssessmentResult.invalid();
    }
  }
}
