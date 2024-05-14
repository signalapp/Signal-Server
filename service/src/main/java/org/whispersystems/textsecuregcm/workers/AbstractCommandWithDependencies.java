/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.cli.Cli;
import io.dropwizard.core.cli.EnvironmentCommand;
import io.dropwizard.core.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;

/**
 * Base class for one-shot commands that use {@link CommandDependencies}.
 * <p>
 * Override {@link #run(Environment, Namespace, WhisperServerConfiguration, CommandDependencies)} in a child class to
 * let the parent class handle common initialization of dependencies, metrics, and logging.
 */
public abstract class AbstractCommandWithDependencies extends EnvironmentCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  protected AbstractCommandWithDependencies(final Application<WhisperServerConfiguration> application,
      final String name, final String description) {
    super(application, name, description);
  }

  /**
   * Run the command with the given initialized {@link CommandDependencies}
   */
  protected abstract void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception;

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {
    UncaughtExceptionHandler.register();
    final CommandDependencies commandDependencies = CommandDependencies.build(getName(), environment, configuration);
    MetricsUtil.configureRegistries(configuration, environment, commandDependencies.dynamicConfigurationManager());

    try {
      logger.info("Starting command dependencies");
      environment.lifecycle().getManagedObjects().forEach(managedObject -> {
        try {
          managedObject.start();
        } catch (final Exception e) {
          logger.error("Failed to start managed object", e);
          throw new RuntimeException(e);
        }
      });

      run(environment, namespace, configuration, commandDependencies);

    } finally {
      logger.info("Stopping command dependencies");
      environment.lifecycle().getManagedObjects().forEach(managedObject -> {
        try {
          managedObject.stop();
        } catch (final Exception e) {
          logger.error("Failed to stop managed object", e);
        }
      });
    }
  }

  @Override
  public void onError(final Cli cli, final Namespace namespace, final Throwable throwable) {
    logger.error("Unhandled error", throwable);
  }
}
