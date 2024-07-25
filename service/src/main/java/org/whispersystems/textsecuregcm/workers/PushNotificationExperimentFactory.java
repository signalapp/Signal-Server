package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;

public interface PushNotificationExperimentFactory<T> {

  PushNotificationExperiment<T> buildExperiment(CommandDependencies commandDependencies,
      WhisperServerConfiguration configuration);
}
