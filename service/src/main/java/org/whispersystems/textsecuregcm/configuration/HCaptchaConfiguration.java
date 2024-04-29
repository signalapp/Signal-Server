/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.whispersystems.textsecuregcm.captcha.HCaptchaClient;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import java.util.concurrent.ScheduledExecutorService;

@JsonTypeName("default")
public class HCaptchaConfiguration implements HCaptchaClientFactory {

  @JsonProperty
  @NotNull
  SecretString apiKey;

  @JsonProperty
  @NotNull
  CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @JsonProperty
  @NotNull
  RetryConfiguration retry = new RetryConfiguration();


  public SecretString getApiKey() {
    return apiKey;
  }

  public CircuitBreakerConfiguration getCircuitBreaker() {
    return circuitBreaker;
  }

  public RetryConfiguration getRetry() {
    return retry;
  }

  @Override
  public HCaptchaClient build(final ScheduledExecutorService retryExecutor,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    return new HCaptchaClient(
        apiKey.value(),
        retryExecutor,
        circuitBreaker,
        retry,
        dynamicConfigurationManager);
  }
}
