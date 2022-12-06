/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class HCaptchaClient implements CaptchaClient {

  private static final Logger logger = LoggerFactory.getLogger(HCaptchaClient.class);
  private static final String PREFIX = "signal-hcaptcha";
  private static final String ASSESSMENT_REASON_COUNTER_NAME = name(HCaptchaClient.class, "assessmentReason");
  private static final String INVALID_REASON_COUNTER_NAME = name(HCaptchaClient.class, "invalidReason");
  private final String apiKey;
  private final HttpClient client;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public HCaptchaClient(
      final String apiKey,
      final HttpClient client,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.apiKey = apiKey;
    this.client = client;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public String scheme() {
    return PREFIX;
  }

  @Override
  public AssessmentResult verify(final String siteKey, final @Nullable String action, final String token,
      final String ip)
      throws IOException {

    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    if (!config.isAllowHCaptcha()) {
      logger.warn("Received request to verify an hCaptcha, but hCaptcha is not enabled");
      return AssessmentResult.invalid();
    }

    final String body = String.format("response=%s&secret=%s&remoteip=%s",
        URLEncoder.encode(token, StandardCharsets.UTF_8),
        URLEncoder.encode(this.apiKey, StandardCharsets.UTF_8),
        ip);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("https://hcaptcha.com/siteverify"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();

    HttpResponse<String> response;
    try {
      response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    if (response.statusCode() != Response.Status.OK.getStatusCode()) {
      logger.warn("failure submitting token to hCaptcha (code={}): {}", response.statusCode(), response);
      throw new IOException("hCaptcha http failure : " + response.statusCode());
    }

    final HCaptchaResponse hCaptchaResponse = SystemMapper.getMapper()
        .readValue(response.body(), HCaptchaResponse.class);

    logger.debug("received hCaptcha response: {}", hCaptchaResponse);

    if (!hCaptchaResponse.success) {
      for (String errorCode : hCaptchaResponse.errorCodes) {
        Metrics.counter(INVALID_REASON_COUNTER_NAME,
            "action", String.valueOf(action),
            "reason", errorCode).increment();
      }
      return AssessmentResult.invalid();
    }

    // hcaptcha uses the inverse scheme of recaptcha (for hcaptcha, a low score is less risky)
    float score = 1.0f - hCaptchaResponse.score;
    if (score < 0.0f || score > 1.0f) {
      logger.error("Invalid score {} from hcaptcha response {}", hCaptchaResponse.score, hCaptchaResponse);
      return AssessmentResult.invalid();
    }
    final String scoreString = AssessmentResult.scoreString(score);

    for (String reason : hCaptchaResponse.scoreReasons) {
      Metrics.counter(ASSESSMENT_REASON_COUNTER_NAME,
          "action", String.valueOf(action),
          "reason", reason,
          "score", scoreString).increment();
    }
    return new AssessmentResult(score >= config.getScoreFloor().floatValue(), scoreString);
  }
}
