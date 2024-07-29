package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSample;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSamples;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FinishPushNotificationExperimentCommandTest {

  private CommandDependencies commandDependencies;
  private PushNotificationExperiment<String> experiment;

  private FinishPushNotificationExperimentCommand<String> finishPushNotificationExperimentCommand;

  private static final String EXPERIMENT_NAME = "test";

  private static class TestFinishPushNotificationExperimentCommand extends FinishPushNotificationExperimentCommand<String> {

    public TestFinishPushNotificationExperimentCommand(final PushNotificationExperiment<String> experiment) {
      super("test-finish-push-notification-experiment",
          "Test start push notification experiment command",
          (ignoredDependencies, ignoredConfiguration) -> experiment);
    }
  }

  @BeforeEach
  void setUp() throws JsonProcessingException {
    final AccountsManager accountsManager = mock(AccountsManager.class);

    final PushNotificationExperimentSamples pushNotificationExperimentSamples =
        mock(PushNotificationExperimentSamples.class);

    when(pushNotificationExperimentSamples.recordFinalState(any(), anyByte(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new PushNotificationExperimentSample<>(true, "test", "test")));

    commandDependencies = new CommandDependencies(accountsManager,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        pushNotificationExperimentSamples,
        null,
        null,
        null,
        null,
        null,
        null);

    //noinspection unchecked
    experiment = mock(PushNotificationExperiment.class);
    when(experiment.getExperimentName()).thenReturn(EXPERIMENT_NAME);
    when(experiment.getState(any(), any())).thenReturn("test");

    doAnswer(invocation -> {
      final Flux<PushNotificationExperimentSample<String>> samples = invocation.getArgument(0);
      samples.then().block();

      return null;
    }).when(experiment).analyzeResults(any());

    finishPushNotificationExperimentCommand = new TestFinishPushNotificationExperimentCommand(experiment);
  }

  @Test
  void run() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);

    final Account account = mock(Account.class);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(experiment).getState(account, device);

    verify(commandDependencies.pushNotificationExperimentSamples())
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }

  @Test
  void runMissingAccount() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(experiment).getState(null, null);

    verify(commandDependencies.pushNotificationExperimentSamples())
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }

  @Test
  void runMissingDevice() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Account account = mock(Account.class);
    when(account.getDevice(anyByte())).thenReturn(Optional.empty());

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(experiment).getState(account, null);

    verify(commandDependencies.pushNotificationExperimentSamples())
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }

  @Test
  void runAccountFetchRetry() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);

    final Account account = mock(Account.class);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(commandDependencies.accountsManager(), times(3)).getByAccountIdentifierAsync(accountIdentifier);

    verify(experiment).getState(account, device);

    verify(commandDependencies.pushNotificationExperimentSamples())
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }

  @Test
  void runStoreSampleRetry() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);

    final Account account = mock(Account.class);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    when(commandDependencies.pushNotificationExperimentSamples().recordFinalState(any(), anyByte(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.completedFuture(new PushNotificationExperimentSample<>(true, "test", "test")));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(experiment).getState(account, device);

    verify(commandDependencies.pushNotificationExperimentSamples(), times(3))
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }

  @Test
  void runMissingInitialSample() throws Exception {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);

    final Account account = mock(Account.class);
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));

    when(commandDependencies.accountsManager().getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(commandDependencies.pushNotificationExperimentSamples().getDevicesPendingFinalState(EXPERIMENT_NAME))
        .thenReturn(Flux.just(Tuples.of(accountIdentifier, deviceId)));

    when(commandDependencies.pushNotificationExperimentSamples().recordFinalState(any(), anyByte(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(ConditionalCheckFailedException.builder().build()));

    assertDoesNotThrow(() -> finishPushNotificationExperimentCommand.run(null,
        new Namespace(Map.of(FinishPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1)),
        null,
        commandDependencies));

    verify(experiment).getState(account, device);

    verify(commandDependencies.pushNotificationExperimentSamples())
        .recordFinalState(eq(accountIdentifier), eq(deviceId), eq(EXPERIMENT_NAME), any());
  }
}
