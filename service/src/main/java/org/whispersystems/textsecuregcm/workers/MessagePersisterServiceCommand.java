/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.cli.ServerCommand;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.HttpsConnectorFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.MessagePersister;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import javax.annotation.Nullable;

public class MessagePersisterServiceCommand extends ServerCommand<WhisperServerConfiguration> {

  @Nullable
  private ExecutorService persistQueueExecutorService;

  @Nullable
  private Scheduler persistQueueScheduler;

  private static final String MAX_CONCURRENCY = "maxConcurrency";

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

    // This is a deliberate misnomer for consistency with other service commands that expect a worker count
    subparser.addArgument("--workers")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY)
        .required(true)
        .help("The maximum number of concurrent Redis/DynamoDB operations");
  }

  @Override
  protected void run(Environment environment, Namespace namespace, WhisperServerConfiguration configuration)
      throws Exception {

    UncaughtExceptionHandler.register();

    final CommandDependencies deps = CommandDependencies.build("message-persister-service", environment, configuration);
    MetricsUtil.configureRegistries(configuration, environment, deps.dynamicConfigurationManager());

    if (configuration.getServerFactory() instanceof DefaultServerFactory defaultServerFactory) {
      defaultServerFactory.getApplicationConnectors()
          .forEach(connectorFactory -> {
            if (connectorFactory instanceof HttpsConnectorFactory h) {
              h.setKeyStorePassword(configuration.getTlsKeyStoreConfiguration().password().value());
            }
          });
    }

    persistQueueExecutorService = Executors.newVirtualThreadPerTaskExecutor();
    persistQueueScheduler = Schedulers.fromExecutorService(persistQueueExecutorService, "persistQueue");

    final MessagePersister messagePersister = new MessagePersister(deps.messagesCache(),
        deps.messagesManager(),
        deps.accountsManager(),
        deps.dynamicConfigurationManager(),
        persistQueueScheduler,
        Clock.systemUTC(),
        Duration.ofMinutes(configuration.getMessageCacheConfiguration().getPersistDelayMinutes()),
        namespace.getInt(MAX_CONCURRENCY));

    environment.lifecycle().manage(messagePersister);

    super.run(environment, namespace, configuration);
  }

  @Override
  protected void cleanup() {
    super.cleanup();

    if (persistQueueScheduler != null) {
      persistQueueScheduler.dispose();
    }

    if (persistQueueExecutorService != null) {
      persistQueueExecutorService.shutdown();
    }
  }
}
