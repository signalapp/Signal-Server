package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;

public class DiscardPushNotificationExperimentSamplesCommand extends AbstractCommandWithDependencies {

  private final PushNotificationExperimentFactory<?> experimentFactory;

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final Logger log = LoggerFactory.getLogger(DiscardPushNotificationExperimentSamplesCommand.class);

  public DiscardPushNotificationExperimentSamplesCommand(final String name,
      final String description,
      final PushNotificationExperimentFactory<?> experimentFactory) {

    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, name, description);

    this.experimentFactory = experimentFactory;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");
  }

  @Override
  protected void run(final Environment environment,
      final Namespace namespace,
      final WhisperServerConfiguration configuration,
      final CommandDependencies commandDependencies) throws Exception {

    final PushNotificationExperiment<?> experiment =
        experimentFactory.buildExperiment(commandDependencies, configuration);

    final int maxConcurrency = namespace.getInt(MAX_CONCURRENCY_ARGUMENT);

    commandDependencies.pushNotificationExperimentSamples()
        .discardSamples(experiment.getExperimentName(), maxConcurrency).join();
  }
}
