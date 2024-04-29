/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.concurrent.ScheduledExecutorService;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@JsonTypeName("static")
public class StaticDynamicConfigurationManagerFactory implements DynamicConfigurationManagerFactory {

  @JsonProperty
  @NotEmpty
  private String application;

  @JsonProperty
  @NotEmpty
  private String environment;

  @JsonProperty
  @NotEmpty
  private String configuration;

  @JsonProperty
  @NotBlank
  private String staticConfig;

  @Override
  public <T> DynamicConfigurationManager<T> build(final Class<T> klazz,
      final ScheduledExecutorService scheduledExecutorService, final AwsCredentialsProvider awsCredentialsProvider) {

    return new StaticDynamicConfigurationManager<>(staticConfig, application, environment, configuration,
        awsCredentialsProvider, klazz, scheduledExecutorService);
  }

  private static class StaticDynamicConfigurationManager<T> extends DynamicConfigurationManager<T> {

    private final T configuration;

    public StaticDynamicConfigurationManager(final String config, final String application, final String environment,
        final String configurationName, final AwsCredentialsProvider awsCredentialsProvider,
        final Class<T> configurationClass, final ScheduledExecutorService scheduledExecutorService) {

      super(application, environment, configurationName, awsCredentialsProvider, configurationClass,
          scheduledExecutorService);

      try {
        this.configuration = parseConfiguration(config, configurationClass).orElseThrow();
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public T getConfiguration() {
      return configuration;
    }

    @Override
    public void start() {
      // do nothing
    }
  }
}
