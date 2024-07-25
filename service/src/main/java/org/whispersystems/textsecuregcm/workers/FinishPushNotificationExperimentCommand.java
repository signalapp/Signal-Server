package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSamples;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.UUID;

public class FinishPushNotificationExperimentCommand<T> extends AbstractCommandWithDependencies {

  private final PushNotificationExperimentFactory<T> experimentFactory;

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

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

    final AccountsManager accountsManager = commandDependencies.accountsManager();
    final PushNotificationExperimentSamples pushNotificationExperimentSamples = commandDependencies.pushNotificationExperimentSamples();

    pushNotificationExperimentSamples.getDevicesPendingFinalState(experiment.getExperimentName())
        .flatMap(accountIdentifierAndDeviceId ->
            Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(accountIdentifierAndDeviceId.getT1()))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                .map(maybeAccount -> Tuples.of(accountIdentifierAndDeviceId.getT1(), accountIdentifierAndDeviceId.getT2(), maybeAccount)), maxConcurrency)
        .map(accountIdentifierAndDeviceIdAndMaybeAccount -> {
          final UUID accountIdentifier = accountIdentifierAndDeviceIdAndMaybeAccount.getT1();
          final byte deviceId = accountIdentifierAndDeviceIdAndMaybeAccount.getT2();

          @Nullable final Account account = accountIdentifierAndDeviceIdAndMaybeAccount.getT3()
              .orElse(null);

          @Nullable final Device device = accountIdentifierAndDeviceIdAndMaybeAccount.getT3()
              .flatMap(a -> a.getDevice(deviceId))
              .orElse(null);

          return Tuples.of(accountIdentifier, deviceId, experiment.getState(account, device));
        })
        .flatMap(accountIdentifierAndDeviceIdAndFinalState -> {
          final UUID accountIdentifier = accountIdentifierAndDeviceIdAndFinalState.getT1();
          final byte deviceId = accountIdentifierAndDeviceIdAndFinalState.getT2();
          final T finalState = accountIdentifierAndDeviceIdAndFinalState.getT3();

          return Mono.fromFuture(() -> {
                try {
                  return pushNotificationExperimentSamples.recordFinalState(accountIdentifier, deviceId,
                      experiment.getExperimentName(), finalState);
                } catch (final JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              })
              .onErrorResume(ConditionalCheckFailedException.class, throwable -> Mono.empty())
              .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
              .onErrorResume(throwable -> {
                log.warn("Failed to record final state for {}:{} in experiment {}",
                    accountIdentifier, deviceId, experiment.getExperimentName(), throwable);

                return Mono.empty();
              });
        }, maxConcurrency)
        .then()
        .block();
  }
}
