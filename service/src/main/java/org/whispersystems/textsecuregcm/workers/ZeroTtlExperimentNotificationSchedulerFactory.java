package org.whispersystems.textsecuregcm.workers;

import java.time.Clock;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables;
import org.whispersystems.textsecuregcm.push.ZeroTtlNotificationScheduler;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;

public class ZeroTtlExperimentNotificationSchedulerFactory implements JobSchedulerFactory {

  @Override
  public JobScheduler buildJobScheduler(final CommandDependencies commandDependencies,
      final WhisperServerConfiguration configuration) {
    final DynamoDbTables.TableWithExpiration tableConfiguration = configuration.getDynamoDbTables().getScheduledJobs();

    final Clock clock = Clock.systemUTC();

    return new ZeroTtlNotificationScheduler(
        commandDependencies.accountsManager(),
        commandDependencies.pushNotificationManager(),
        commandDependencies.dynamoDbAsyncClient(),
        tableConfiguration.getTableName(),
        tableConfiguration.getExpiration(),
        clock);
  }
}
