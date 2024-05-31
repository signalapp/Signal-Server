/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import io.dropwizard.util.Resources;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@JsonTypeName("local")
public class LocalDynamicConfigurationManagerFactory implements DynamicConfigurationManagerFactory {

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
  private String configPath;

  @Override
  public <T> DynamicConfigurationManager<T> build(final Class<T> klazz,
      final ScheduledExecutorService scheduledExecutorService, final AwsCredentialsProvider awsCredentialsProvider) {

    return new LocalDynamicConfigurationManager<>(configPath, application, environment, configuration,
        awsCredentialsProvider, klazz, scheduledExecutorService);
  }

  private static class LocalDynamicConfigurationManager<T> extends DynamicConfigurationManager<T> {

    private static final Logger logger = LoggerFactory.getLogger(DynamicConfigurationManager.class);

    private final Path configPath;
    private final Class<T> configurationClass;
    private T cachedConfig;
    private final Instant lastConfigLoadedTime;

    public LocalDynamicConfigurationManager(final String configPath, final String application, final String environment,
        final String configurationName, final AwsCredentialsProvider awsCredentialsProvider,
        final Class<T> configurationClass, final ScheduledExecutorService scheduledExecutorService) {

      super(application, environment, configurationName, awsCredentialsProvider, configurationClass,
          scheduledExecutorService);

      this.configPath = Path.of(Resources.getResource("config").getPath()).resolve(configPath);
      this.configurationClass = configurationClass;
      this.cachedConfig = null;
      this.lastConfigLoadedTime = null;
      maybeUpdateConfig();
      if (cachedConfig == null) {
        throw new IllegalArgumentException("failed to load initial config");
      }
    }

    @Override
    public T getConfiguration() {
      maybeUpdateConfig();
      return cachedConfig;
    }

    @Override
    public void start() {
      // do nothing
    }

    private synchronized void maybeUpdateConfig() {
      try {
        if (lastConfigLoadedTime != null &&
            !lastConfigLoadedTime.isBefore(Files.readAttributes(configPath, BasicFileAttributes.class).lastModifiedTime().toInstant())) {
          return;
        }
        String configContents = Files.readString(configPath);
        parseConfiguration(configContents, configurationClass).ifPresent(config -> cachedConfig = config);
      } catch (Exception e) {
        logger.warn("Failed to update configuration", e);
      }
    }

  }

}
