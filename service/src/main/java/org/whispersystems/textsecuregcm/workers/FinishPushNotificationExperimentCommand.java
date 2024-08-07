package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSample;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSamples;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import java.time.Duration;

public class FinishPushNotificationExperimentCommand<T> extends AbstractCommandWithDependencies {

  private final PushNotificationExperimentFactory<T> experimentFactory;

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final String SAMPLES_READ_COUNTER_NAME =
      MetricsUtil.name(FinishPushNotificationExperimentCommand.class, "samplesRead");

  private static final Counter ACCOUNT_READ_COUNTER =
      Metrics.counter(MetricsUtil.name(FinishPushNotificationExperimentCommand.class, "accountRead"));

  private static final Counter FINAL_SAMPLE_STORED_COUNTER =
      Metrics.counter(MetricsUtil.name(FinishPushNotificationExperimentCommand.class, "finalSampleStored"));

  private static final Logger log = LoggerFactory.getLogger(FinishPushNotificationExperimentCommand.class);

  public FinishPushNotificationExperimentCommand(final String name,
      final String description,
      final PushNotificationExperimentFactory<T> experimentFactory) {

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

    final PushNotificationExperiment<T> experiment =
        experimentFactory.buildExperiment(commandDependencies, configuration);

    final int maxConcurrency = namespace.getInt(MAX_CONCURRENCY_ARGUMENT);

    log.info("Finishing \"{}\" with max concurrency: {}", experiment.getExperimentName(), maxConcurrency);

    final AccountsManager accountsManager = commandDependencies.accountsManager();
    final PushNotificationExperimentSamples pushNotificationExperimentSamples = commandDependencies.pushNotificationExperimentSamples();

    final Flux<PushNotificationExperimentSample<T>> finishedSamples =
        pushNotificationExperimentSamples.getSamples(experiment.getExperimentName(), experiment.getStateClass())
            .doOnNext(sample -> Metrics.counter(SAMPLES_READ_COUNTER_NAME, "final", String.valueOf(sample.finalState() != null)).increment())
            .flatMap(sample -> {
              if (sample.finalState() == null) {
                // We still need to record a final state for this sample
                return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(sample.accountIdentifier()))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .doOnNext(ignored -> ACCOUNT_READ_COUNTER.increment())
                    .flatMap(maybeAccount -> {
                      final T finalState = experiment.getState(maybeAccount.orElse(null),
                          maybeAccount.flatMap(account -> account.getDevice(sample.deviceId())).orElse(null));

                      return Mono.fromFuture(
                              () -> pushNotificationExperimentSamples.recordFinalState(sample.accountIdentifier(),
                                  sample.deviceId(),
                                  experiment.getExperimentName(),
                                  finalState))
                          .onErrorResume(ConditionalCheckFailedException.class, throwable -> Mono.empty())
                          .onErrorResume(JsonProcessingException.class, throwable -> {
                            log.error("Failed to parse sample state JSON", throwable);
                            return Mono.empty();
                          })
                          .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                          .onErrorResume(throwable -> {
                            log.warn("Failed to record final state for {}:{} in experiment {}",
                                sample.accountIdentifier(), sample.deviceId(), experiment.getExperimentName(), throwable);

                            return Mono.empty();
                          })
                          .doOnSuccess(ignored -> FINAL_SAMPLE_STORED_COUNTER.increment());
                    });
              } else {
                return Mono.just(sample);
              }
            }, maxConcurrency);

    experiment.analyzeResults(finishedSamples);
  }
}
