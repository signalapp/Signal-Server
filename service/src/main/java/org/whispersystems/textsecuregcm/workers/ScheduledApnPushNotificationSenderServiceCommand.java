/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.core.Application;
import io.dropwizard.core.cli.ServerCommand;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.HttpsConnectorFactory;
import java.util.concurrent.ExecutorService;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnPushNotificationScheduler;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;

public class ScheduledApnPushNotificationSenderServiceCommand extends ServerCommand<WhisperServerConfiguration> {

  private static final String WORKER_COUNT = "workers";

  public ScheduledApnPushNotificationSenderServiceCommand() {
    super(new Application<>() {
            @Override
            public void run(WhisperServerConfiguration configuration, Environment environment) {

            }
          }, "scheduled-apn-push-notification-sender-service",
        "Starts a persistent service to send scheduled APNs push notifications");
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

    final CommandDependencies deps = CommandDependencies.build("scheduled-apn-sender", environment, configuration);

    if (configuration.getServerFactory() instanceof DefaultServerFactory defaultServerFactory) {
      defaultServerFactory.getApplicationConnectors()
          .forEach(connectorFactory -> {
            if (connectorFactory instanceof HttpsConnectorFactory h) {
              h.setKeyStorePassword(configuration.getTlsKeyStoreConfiguration().password().value());
            }
          });
    }

    final FaultTolerantRedisCluster pushSchedulerCluster = new FaultTolerantRedisCluster("push_scheduler",
        configuration.getPushSchedulerCluster(), deps.redisClusterClientResources());

    final ExecutorService apnSenderExecutor = environment.lifecycle().executorService(name(getClass(), "apnSender-%d"))
        .maxThreads(1).minThreads(1).build();

    final APNSender apnSender = new APNSender(apnSenderExecutor, configuration.getApnConfiguration());
    final ApnPushNotificationScheduler apnPushNotificationScheduler = new ApnPushNotificationScheduler(
        pushSchedulerCluster, apnSender, deps.accountsManager(), namespace.getInt(WORKER_COUNT));

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(apnPushNotificationScheduler);

    MetricsUtil.registerSystemResourceMetrics(environment);

    super.run(environment, namespace, configuration);
  }

}
