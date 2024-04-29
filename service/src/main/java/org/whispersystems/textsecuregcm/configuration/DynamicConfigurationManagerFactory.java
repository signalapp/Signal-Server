/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.util.concurrent.ScheduledExecutorService;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = AppConfigConfiguration.class)
public interface DynamicConfigurationManagerFactory extends Discoverable {

  <T> DynamicConfigurationManager<T> build(Class<T> configurationClass,
      ScheduledExecutorService scheduledExecutorService, AwsCredentialsProvider awsCredentialsProvider);
}
