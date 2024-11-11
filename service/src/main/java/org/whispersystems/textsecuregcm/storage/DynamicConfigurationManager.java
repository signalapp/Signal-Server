/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class DynamicConfigurationManager<T> {

  private final S3ObjectMonitor configMonitor;
  private final Class<T> configurationClass;

  // Set on initial config fetch
  private final AtomicReference<T> configuration = new AtomicReference<>();
  private final CountDownLatch initialized = new CountDownLatch(1);

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  private static final String ERROR_COUNTER_NAME = name(DynamicConfigurationManager.class, "error");
  private static final String ERROR_TYPE_TAG_NAME = "type";
  private static final String CONFIG_CLASS_TAG_NAME = "configClass";

  private static final Logger logger = LoggerFactory.getLogger(DynamicConfigurationManager.class);

  public DynamicConfigurationManager(final S3ObjectMonitor configMonitor, final Class<T> configurationClass) {
    this.configMonitor = configMonitor;
    this.configurationClass = configurationClass;
  }

  public T getConfiguration() {
    try {
      initialized.await();
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for initial configuration", e);
      throw new RuntimeException(e);
    }
    return configuration.get();
  }

  public synchronized void start() {
    if (initialized.getCount() == 0) {
      return;
    }

    this.configMonitor.start(this::receiveConfiguration);

    // Starting an S3ObjectMonitor immediately does a blocking retrieve of the data, but it might
    // fail to parse, in which case we wait for an update (which will happen on another thread) to
    // give us a valid configuration before marking ourselves ready
    while (configuration.get() == null) {
      logger.warn("Failed to retrieve or parse initial dynamic configuration");
      try {
        this.wait();
      } catch (InterruptedException e) {}
    }
    initialized.countDown();
  }

  private synchronized void receiveConfiguration(InputStream configDataStream) {
    final String configData;
    try {
      configData = new String(configDataStream.readAllBytes());
    } catch (IOException e) {
      Metrics.counter(ERROR_COUNTER_NAME, ERROR_TYPE_TAG_NAME, "fetch").increment();
      return;
    }

    logger.info("Received new dynamic configuration of length {}", configData.length());
    parseConfiguration(configData, configurationClass).ifPresent(configuration::set);
    this.notify();
  }

  @VisibleForTesting
  public static <T> Optional<T> parseConfiguration(final String configurationYaml, final Class<T> configurationClass) {
    final T configuration;
    try {
      configuration = SystemMapper.yamlMapper().readValue(configurationYaml, configurationClass);
    } catch (final IOException e) {
      logger.warn("Failed to parse dynamic configuration", e);
      Metrics.counter(ERROR_COUNTER_NAME,
          ERROR_TYPE_TAG_NAME, "parse",
          CONFIG_CLASS_TAG_NAME, configurationClass.getName()).increment();
      return Optional.empty();
    }

    final Set<ConstraintViolation<T>> violations = VALIDATOR.validate(configuration);

    if (!violations.isEmpty()) {
      logger.warn("Failed to validate configuration: {}", violations);
      Metrics.counter(ERROR_COUNTER_NAME,
          ERROR_TYPE_TAG_NAME, "validate",
          CONFIG_CLASS_TAG_NAME, configurationClass.getName()).increment();
      return Optional.empty();
    }

    return Optional.of(configuration);
  }

}
