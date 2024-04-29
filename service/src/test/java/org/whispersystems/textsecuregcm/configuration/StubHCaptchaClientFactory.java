/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.whispersystems.textsecuregcm.captcha.Action;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.HCaptchaClient;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

@JsonTypeName("stub")
public class StubHCaptchaClientFactory implements HCaptchaClientFactory {

  @Override
  public HCaptchaClient build(final ScheduledExecutorService retryExecutor,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    return new StubHCaptchaClient(retryExecutor, new CircuitBreakerConfiguration(), dynamicConfigurationManager);
  }

  /**
   * Accepts any token of the format "test.test.*.*"
   */
  private static class StubHCaptchaClient extends HCaptchaClient {

    public StubHCaptchaClient(final ScheduledExecutorService retryExecutor,
        final CircuitBreakerConfiguration circuitBreakerConfiguration,
        final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
      super(null, retryExecutor, circuitBreakerConfiguration, null, dynamicConfigurationManager);
    }

    @Override
    public String scheme() {
      return "test";
    }

    @Override
    public Set<String> validSiteKeys(final Action action) {
      return Set.of("test");
    }

    @Override
    public AssessmentResult verify(final String siteKey, final Action action, final String token, final String ip)
        throws IOException {
      return AssessmentResult.alwaysValid();
    }
  }
}
