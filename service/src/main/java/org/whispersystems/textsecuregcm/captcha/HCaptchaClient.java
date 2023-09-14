/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class HCaptchaClient implements CaptchaClient {

  private static final Logger logger = LoggerFactory.getLogger(HCaptchaClient.class);
  private static final String PREFIX = "signal-hcaptcha";
  private static final String ASSESSMENT_REASON_COUNTER_NAME = name(HCaptchaClient.class, "assessmentReason");
  private static final String INVALID_REASON_COUNTER_NAME = name(HCaptchaClient.class, "invalidReason");
  private final String apiKey;
  private final FaultTolerantHttpClient client;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  @VisibleForTesting
  HCaptchaClient(final String apiKey,
      final FaultTolerantHttpClient faultTolerantHttpClient,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.apiKey = apiKey;
    this.client = faultTolerantHttpClient;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public HCaptchaClient(
      final String apiKey,
      final ScheduledExecutorService retryExecutor,
      final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final RetryConfiguration retryConfiguration,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this(apiKey,
        FaultTolerantHttpClient.newBuilder()
            .withName("hcaptcha")
            .withCircuitBreaker(circuitBreakerConfiguration)
            .withExecutor(Executors.newCachedThreadPool())
            .withRetryExecutor(retryExecutor)
            .withRetry(retryConfiguration)
            .withRetryOnException(ex -> ex instanceof IOException)
            .withConnectTimeout(Duration.ofSeconds(10))
            .withVersion(HttpClient.Version.HTTP_2)
            .build(),
        dynamicConfigurationManager);
  }

  @Override
  public String scheme() {
    return PREFIX;
  }

  @Override
  public Set<String> validSiteKeys(final Action action) {
    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    if (!config.isAllowHCaptcha()) {
      logger.warn("Received request to verify an hCaptcha, but hCaptcha is not enabled");
      return Collections.emptySet();
    }
    return Optional
        .ofNullable(config.getHCaptchaSiteKeys().get(action))
        .orElse(Collections.emptySet());
  }

  @Override
  public AssessmentResult verify(
      final String siteKey,
      final Action action,
      final String token,
      final String ip)
      throws IOException {

    final DynamicCaptchaConfiguration config = dynamicConfigurationManager.getConfiguration().getCaptchaConfiguration();
    final String body = String.format("response=%s&secret=%s&remoteip=%s",
        URLEncoder.encode(token, StandardCharsets.UTF_8),
        URLEncoder.encode(this.apiKey, StandardCharsets.UTF_8),
        ip);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("https://hcaptcha.com/siteverify"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();

    final HttpResponse<String> response;
    try {
      response = this.client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
    } catch (CompletionException e) {
      logger.warn("failed to make http request to hCaptcha: {}", e.getMessage());
      throw new IOException(ExceptionUtils.unwrap(e));
    }

    if (response.statusCode() != Response.Status.OK.getStatusCode()) {
      logger.warn("failure submitting token to hCaptcha (code={}): {}", response.statusCode(), response);
      throw new IOException("hCaptcha http failure : " + response.statusCode());
    }

    final HCaptchaResponse hCaptchaResponse = SystemMapper.jsonMapper()
        .readValue(response.body(), HCaptchaResponse.class);

    logger.debug("received hCaptcha response: {}", hCaptchaResponse);

    if (!hCaptchaResponse.success) {
      for (String errorCode : hCaptchaResponse.errorCodes) {
        Metrics.counter(INVALID_REASON_COUNTER_NAME,
            "action", action.getActionName(),
            "reason", errorCode).increment();
      }
      return AssessmentResult.invalid();
    }

    // hcaptcha uses the inverse scheme of recaptcha (for hcaptcha, a low score is less risky)
    final float score = 1.0f - hCaptchaResponse.score;
    if (score < 0.0f || score > 1.0f) {
      logger.error("Invalid score {} from hcaptcha response {}", hCaptchaResponse.score, hCaptchaResponse);
      return AssessmentResult.invalid();
    }
    final BigDecimal threshold = config.getScoreFloorByAction().getOrDefault(action, config.getScoreFloor());
    final AssessmentResult assessmentResult = AssessmentResult.fromScore(score, threshold.floatValue());

    for (String reason : hCaptchaResponse.scoreReasons) {
      Metrics.counter(ASSESSMENT_REASON_COUNTER_NAME,
          "action", action.getActionName(),
          "reason", reason,
          "score", assessmentResult.getScoreString()).increment();
    }
    return assessmentResult;
  }
}
