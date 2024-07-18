package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;

public interface JobSchedulerFactory {

  JobScheduler buildJobScheduler(CommandDependencies commandDependencies, WhisperServerConfiguration configuration);
}
