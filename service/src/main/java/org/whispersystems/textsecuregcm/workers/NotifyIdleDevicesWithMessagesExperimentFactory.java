package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables;
import org.whispersystems.textsecuregcm.experiment.DeviceLastSeenState;
import org.whispersystems.textsecuregcm.experiment.NotifyIdleDevicesWithMessagesExperiment;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import java.time.Clock;

public class NotifyIdleDevicesWithMessagesExperimentFactory implements PushNotificationExperimentFactory<DeviceLastSeenState> {

  @Override
  public PushNotificationExperiment<DeviceLastSeenState> buildExperiment(final CommandDependencies commandDependencies,
      final WhisperServerConfiguration configuration) {

    final DynamoDbTables.TableWithExpiration tableConfiguration = configuration.getDynamoDbTables().getScheduledJobs();

    final Clock clock = Clock.systemUTC();

    return new NotifyIdleDevicesWithMessagesExperiment(new IdleDeviceNotificationScheduler(
            commandDependencies.accountsManager(),
            commandDependencies.pushNotificationManager(),
            commandDependencies.dynamoDbAsyncClient(),
            tableConfiguration.getTableName(),
            tableConfiguration.getExpiration(),
            clock),
        commandDependencies.messagesManager(),
        clock);
  }
}
