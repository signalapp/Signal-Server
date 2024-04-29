/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import org.whispersystems.textsecuregcm.captcha.HCaptchaClient;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import java.util.concurrent.ScheduledExecutorService;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = HCaptchaConfiguration.class)
public interface HCaptchaClientFactory extends Discoverable {

  HCaptchaClient build(ScheduledExecutorService retryExecutor,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager);
}
