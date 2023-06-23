/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.Application;
import io.dropwizard.cli.ServerCommand;
import io.dropwizard.setup.Environment;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagePersister;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;

public class MessagePersisterServiceCommand extends ServerCommand<WhisperServerConfiguration> {

  private static final String WORKER_COUNT = "workers";

  public MessagePersisterServiceCommand() {
    super(new Application<>() {
            @Override
            public void run(WhisperServerConfiguration configuration, Environment environment) {

            }
          }, "message-persister-service",
        "Starts a persistent service to persist undelivered messages from Redis to Dynamo DB");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("--workers")
        .type(Integer.class)
        .dest(WORKER_COUNT)
        .required(true)
        .help("The number of worker threads");
  }

  @Override
  protected void run(Environment environment, Namespace namespace, WhisperServerConfiguration configuration)
      throws Exception {

    UncaughtExceptionHandler.register();

    MetricsUtil.configureRegistries(configuration, environment);

    final CommandDependencies deps = CommandDependencies.build("message-persister-service", environment, configuration);

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new DynamicConfigurationManager<>(
        configuration.getAppConfig().getApplication(),
        configuration.getAppConfig().getEnvironment(),
        configuration.getAppConfig().getConfigurationName(),
        DynamicConfiguration.class);

    dynamicConfigurationManager.start();

    final MessagePersister messagePersister = new MessagePersister(deps.messagesCache(), deps.messagesManager(),
        deps.accountsManager(),
        dynamicConfigurationManager,
        Duration.ofMinutes(configuration.getMessageCacheConfiguration().getPersistDelayMinutes()),
        namespace.getInt(WORKER_COUNT));

    environment.lifecycle().manage(deps.messagesCache());
    environment.lifecycle().manage(messagePersister);

    MetricsUtil.registerSystemResourceMetrics(environment);

    super.run(environment, namespace, configuration);
  }

}
