/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jackson.Discoverable;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RegistrationServiceConfiguration.class)
public interface RegistrationServiceClientFactory extends Discoverable {

  RegistrationServiceClient build(Environment environment, Executor callbackExecutor,
      ScheduledExecutorService identityRefreshExecutor);
}
