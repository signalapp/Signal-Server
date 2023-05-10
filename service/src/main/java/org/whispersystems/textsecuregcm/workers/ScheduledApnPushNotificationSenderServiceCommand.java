/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnPushNotificationScheduler;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class ScheduledApnPushNotificationSenderServiceCommand extends EnvironmentCommand<WhisperServerConfiguration> {

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

    final CommandDependencies deps = CommandDependencies.build("scheduled-apn-sender", environment, configuration);

    final FaultTolerantRedisCluster pushSchedulerCluster = new FaultTolerantRedisCluster("push_scheduler",
        configuration.getPushSchedulerCluster(), deps.redisClusterClientResources());

    final ExecutorService apnSenderExecutor = environment.lifecycle().executorService(name(getClass(), "apnSender-%d"))
        .maxThreads(1).minThreads(1).build();

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new DynamicConfigurationManager<>(
        configuration.getAppConfig().getApplication(),
        configuration.getAppConfig().getEnvironment(),
        configuration.getAppConfig().getConfigurationName(),
        DynamicConfiguration.class);

    final APNSender apnSender = new APNSender(apnSenderExecutor, configuration.getApnConfiguration());
    final ApnPushNotificationScheduler apnPushNotificationScheduler = new ApnPushNotificationScheduler(
        pushSchedulerCluster, apnSender, deps.accountsManager(), Optional.of(namespace.getInt(WORKER_COUNT)),
        dynamicConfigurationManager);

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(apnPushNotificationScheduler);
  }

}
