package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperiment;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSamples;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;

class StartPushNotificationExperimentCommandTest {

  private PushNotificationExperimentSamples pushNotificationExperimentSamples;
  private PushNotificationExperiment<String> experiment;

  private TestStartPushNotificationExperimentCommand startPushNotificationExperimentCommand;

  // Taken together, these parameters will produce a device that's enrolled in the experimental group (as opposed to the
  // control group) for an experiment.
  private static final UUID ACCOUNT_IDENTIFIER = UUID.fromString("341fb18f-9dee-4181-bc40-e485958341d3");
  private static final byte DEVICE_ID = Device.PRIMARY_ID;
  private static final String EXPERIMENT_NAME = "test";

  private static class TestStartPushNotificationExperimentCommand extends StartPushNotificationExperimentCommand<String> {

    private final CommandDependencies commandDependencies;
    private boolean dryRun = false;

    public TestStartPushNotificationExperimentCommand(
        final PushNotificationExperimentSamples pushNotificationExperimentSamples,
        final PushNotificationExperiment<String> experiment) {

      super("test-start-push-notification-experiment",
          "Test start push notification experiment command",
          (ignoredDependencies, ignoredConfiguration) -> experiment);

      this.commandDependencies = new CommandDependencies(null,
          null,
          null,
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
          null,
          null,
          null,
          null);
    }

    void setDryRun(final boolean dryRun) {
      this.dryRun = dryRun;
    }

    @Override
    protected Namespace getNamespace() {
      return new Namespace(Map.of(
          StartPushNotificationExperimentCommand.MAX_CONCURRENCY_ARGUMENT, 1,
          StartPushNotificationExperimentCommand.DRY_RUN_ARGUMENT, dryRun));
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return commandDependencies;
    }
  }

  @BeforeEach
  void setUp() {
    //noinspection unchecked
    experiment = mock(PushNotificationExperiment.class);
    when(experiment.getExperimentName()).thenReturn(EXPERIMENT_NAME);
    when(experiment.isDeviceEligible(any(), any())).thenReturn(CompletableFuture.completedFuture(true));
    when(experiment.getState(any(), any())).thenReturn("test");
    when(experiment.applyExperimentTreatment(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    pushNotificationExperimentSamples = mock(PushNotificationExperimentSamples.class);

    try {
      when(pushNotificationExperimentSamples.recordInitialState(any(), anyByte(), any(), anyBoolean(), any()))
          .thenReturn(CompletableFuture.completedFuture(true));
    } catch (final JsonProcessingException e) {
      throw new AssertionError(e);
    }

    startPushNotificationExperimentCommand =
        new TestStartPushNotificationExperimentCommand(pushNotificationExperimentSamples, experiment);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    startPushNotificationExperimentCommand.setDryRun(dryRun);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTIFIER);
    when(account.getDevices()).thenReturn(List.of(device));

    assertDoesNotThrow(() -> startPushNotificationExperimentCommand.crawlAccounts(Flux.just(account)));

    if (dryRun) {
      verify(experiment, never()).applyExperimentTreatment(any(), any());
    } else {
      verify(experiment).applyExperimentTreatment(account, device);
    }

    verify(experiment, never()).applyControlTreatment(account, device);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccountsExistingSample(final boolean dryRun) throws JsonProcessingException {
    startPushNotificationExperimentCommand.setDryRun(dryRun);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTIFIER);
    when(account.getDevices()).thenReturn(List.of(device));

    when(pushNotificationExperimentSamples.recordInitialState(any(), anyByte(), any(), anyBoolean(), any()))
        .thenReturn(CompletableFuture.completedFuture(false));

    assertDoesNotThrow(() -> startPushNotificationExperimentCommand.crawlAccounts(Flux.just(account)));
    verify(experiment, never()).applyExperimentTreatment(any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccountsSampleRetry(final boolean dryRun) throws JsonProcessingException {
    startPushNotificationExperimentCommand.setDryRun(dryRun);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTIFIER);
    when(account.getDevices()).thenReturn(List.of(device));

    when(pushNotificationExperimentSamples.recordInitialState(any(), anyByte(), any(), anyBoolean(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.completedFuture(true));

    assertDoesNotThrow(() -> startPushNotificationExperimentCommand.crawlAccounts(Flux.just(account)));

    if (dryRun) {
      verify(experiment, never()).applyExperimentTreatment(any(), any());
      verify(pushNotificationExperimentSamples, never())
          .recordInitialState(any(), anyByte(), any(), anyBoolean(), any());
    } else {
      verify(experiment).applyExperimentTreatment(account, device);
      verify(pushNotificationExperimentSamples, times(3))
          .recordInitialState(ACCOUNT_IDENTIFIER, DEVICE_ID, EXPERIMENT_NAME, true, "test");
    }

    verify(experiment, never()).applyControlTreatment(account, device);
  }

  @Test
  void crawlAccountsExperimentException() {
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTIFIER);
    when(account.getDevices()).thenReturn(List.of(device));

    when(experiment.applyExperimentTreatment(account, device))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    assertDoesNotThrow(() -> startPushNotificationExperimentCommand.crawlAccounts(Flux.just(account)));
    verify(experiment).applyExperimentTreatment(account, device);
  }
}
