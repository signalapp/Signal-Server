/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables;
import org.whispersystems.textsecuregcm.experiment.DeviceLastSeenState;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.experiment.ZeroTtlPushNotificationExperiment;
import org.whispersystems.textsecuregcm.push.ZeroTtlNotificationScheduler;
import java.time.Clock;

public class ZeroTtlPushNotificationExperimentFactory implements PushNotificationExperimentFactory<DeviceLastSeenState> {

  @Override
  public PushNotificationExperiment<DeviceLastSeenState> buildExperiment(final CommandDependencies commandDependencies,
      final WhisperServerConfiguration configuration) {

    final DynamoDbTables.TableWithExpiration tableConfiguration = configuration.getDynamoDbTables().getScheduledJobs();

    final Clock clock = Clock.systemUTC();

    return new ZeroTtlPushNotificationExperiment(
        new IdleWakeupEligibilityChecker(clock, commandDependencies.messagesManager()),
        new ZeroTtlNotificationScheduler(
        commandDependencies.accountsManager(),
        commandDependencies.pushNotificationManager(),
        commandDependencies.dynamoDbAsyncClient(),
        tableConfiguration.getTableName(),
        tableConfiguration.getExpiration(),
        clock));
  }
}
